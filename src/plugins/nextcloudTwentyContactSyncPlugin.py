"""Contact reconciliation plugin: three-way sync between Twenty (People) and
Nextcloud (Global address book), using AutoKB output files as the reconciled
source of truth.

Identity is tracked via the per-source stable ids (Nextcloud card href and
Twenty person id) stored on the AutoKB record. Email is used only to link
records that have no established id linkage (happy path). Field changes - e.g.
an email or last name change - are ordinary updates, never a new identity.

Rules:
  * Nextcloud always wins when both sources differ from AutoKB.
  * A record missing from exactly one source is reported (never auto-deleted
    or re-created); when missing from both, the AutoKB file is removed.
  * Anything abnormal outside the simple happy path is reported, never acted on.
  * Records with no email are silently skipped.
  * Companies are linked as Twenty Company objects (Twenty is the source of
    truth for the linkage); AutoKB records the company id and mirrors the
    company name to Nextcloud as the vCard ORG field.

Email is sent via utils.misc_utils.send_smtp_notification, which reuses the
backend SMTP configuration (env) - no SMTP is configured in the schema.
"""

import hashlib
import json
import os
import uuid

import phonenumbers
import requests

from utils.misc_utils import SubscriptionCancelledError, send_smtp_notification
from utils.plugin_base import BaseSubscription


COMPARE_FIELDS = ["firstName", "lastName", "email", "phone", "jobTitle", "company"]
OTHER_FIELDS = [f for f in COMPARE_FIELDS if f != "company"]


def _clean(value):
    return "" if value is None else str(value).strip()


def _canonical_phone(raw):
    """Normalize a phone to a stable E.164 string (strips formatting) while
    preserving the country code, so NC and Twenty values compare identically."""
    value = _clean(raw)
    if value.startswith("+"):
        try:
            return phonenumbers.format_number(
                phonenumbers.parse(value, None), phonenumbers.PhoneNumberFormat.E164
            )
        except phonenumbers.NumberParseException:
            pass
    return "".join(ch for ch in value if ch.isdigit() or ch == "+")


def _split_phone(number):
    """Resolve a canonical phone back into Twenty's composite pieces. Calling
    and country codes default to the caller-code region; if the number cannot
    be parsed they are omitted (best effort, never raises)."""
    canonical = _canonical_phone(number)
    if canonical.startswith("+"):
        try:
            parsed = phonenumbers.parse(canonical, None)
            return {
                "primaryPhoneNumber": str(parsed.national_number),
                "primaryPhoneCallingCode": f"+{parsed.country_code}",
                "primaryPhoneCountryCode": phonenumbers.region_code_for_number(
                    parsed
                )
                or "",
            }
        except phonenumbers.NumberParseException:
            pass
    return {
        "primaryPhoneNumber": canonical,
        "primaryPhoneCallingCode": "",
        "primaryPhoneCountryCode": "",
    }


def _ak_key(record):
    """Stable filename stem: sha256 of the Twenty id (never email-derived)."""
    return hashlib.sha256(str(record["twentyId"]).encode("utf-8")).hexdigest()


def _compare_other(record):
    return {f: _clean(record.get(f)) for f in OTHER_FIELDS}


def _same_other(a, b):
    return _compare_other(a) == _compare_other(b)


def _clone_compare(source):
    rec = {f: _clean(source.get(f)) for f in COMPARE_FIELDS}
    rec["companyId"] = source.get("companyId")
    return rec


def _display(record):
    full = f"{_clean(record.get('firstName'))} {_clean(record.get('lastName'))}".strip()
    if record.get("email"):
        full = f"{full} <{record['email']}>".strip()
    return full or "unknown"


def _unescape_vcard(value):
    value = value.replace("\\n", "\n").replace("\\,", ",").replace("\\;", ";")
    value = value.replace("\\\\", "\\")
    return value


def _parse_vcard(text):
    """Best-effort vCard 3.0 parser returning the fields we care about."""
    lines = []
    for raw in text.replace("\r\n", "\n").split("\n"):
        line = raw.rstrip("\r")
        if line[:1] in (" ", "\t"):
            if lines:
                lines[-1] += line[1:]
            continue
        lines.append(line)

    props = {}
    for line in lines:
        if ":" not in line:
            continue
        prop_part, _, value = line.partition(":")
        name = prop_part.split(";")[0].strip().upper()
        if name and name not in props:
            props[name] = _unescape_vcard(value)

    first, last = "", ""
    n = props.get("N", "")
    if ";" in n:
        parts = n.split(";")
        last = _clean(parts[0])
        first = _clean(parts[1])
    else:
        fn_parts = _clean(props.get("FN", "")).split(" ", 1)
        first, last = (fn_parts[0], fn_parts[1] if len(fn_parts) > 1 else "")

    org = _clean(props.get("ORG", "")).split(";")[0]
    return {
        "firstName": first,
        "lastName": last,
        "email": _clean(props.get("EMAIL", "")).lower(),
        "phone": _canonical_phone(props.get("TEL", "")),
        "jobTitle": _clean(props.get("TITLE", "")),
        "company": org,
    }


def _serialize_vcard(record, uid):
    pieces = ["BEGIN:VCARD", "VERSION:3.0", f"UID:{uid}"]
    last = _clean(record.get("lastName")).replace(",", "\\,").replace(";", "\\;")
    first = _clean(record.get("firstName")).replace(",", "\\,").replace(";", "\\;")
    pieces.append(f"N:{last};{first};;;")
    full = f"{first} {last}".strip()
    if full:
        pieces.append(f"FN:{full}")
    if record.get("email"):
        pieces.append(f"EMAIL;TYPE=WORK:{_clean(record['email'])}")
    if record.get("phone"):
        pieces.append(f"TEL;TYPE=CELL:{_clean(record['phone'])}")
    if record.get("jobTitle"):
        pieces.append(f"TITLE:{_clean(record['jobTitle'])}")
    if record.get("company"):
        pieces.append(f"ORG:{_clean(record['company'])}")
    pieces.append("END:VCARD")
    return "\r\n".join(pieces) + "\r\n"


def _abs_url(origin, href):
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return origin + href
    return origin + "/" + href


def _href_path(href):
    """Normalize a Nextcloud card href to its relative-path form (as PROPFIND returns)."""
    if href.startswith("http"):
        after = href.split("://", 1)[1]
        sep = after.find("/")
        return after[sep:] if sep >= 0 else "/"
    return href


def _multistatus_cards(text):
    import xml.etree.ElementTree as ET

    cards = []
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return cards
    for resp in root.iter("{DAV:}response"):
        href_el = resp.find("{DAV:}href")
        if href_el is None or not href_el.text:
            continue
        href = href_el.text
        etag = ""
        card_data = ""
        for propstat in resp.findall("{DAV:}propstat"):
            prop = propstat.find("{DAV:}prop")
            if prop is None:
                continue
            etag_el = prop.find("{DAV:}getetag")
            if etag_el is not None and etag_el.text:
                etag = etag_el.text.strip()
            data_el = prop.find("{urn:ietf:params:xml:ns:carddav}address-data")
            if data_el is not None and data_el.text:
                card_data = data_el.text
        cards.append((href, etag, card_data))
    return cards


def _is_addressbook(text):
    """Return True if a PROPFIND multistatus contains a CardDAV addressbook."""
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return False
    for resp in root.iter("{DAV:}response"):
        for propstat in resp.findall("{DAV:}propstat"):
            prop = propstat.find("{DAV:}prop")
            if prop is None:
                continue
            rt = prop.find("{DAV:}resourcetype")
            if rt is not None and rt.find(
                "{urn:ietf:params:xml:ns:carddav}addressbook"
            ) is not None:
                return True
    return False


def _esc(value):
    """Minimal XML text escaping."""
    value = _clean(value)
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _extract_id(resp_data):
    if isinstance(resp_data, dict):
        if resp_data.get("id"):
            return resp_data["id"]
        payload = resp_data.get("data")
        if isinstance(payload, dict):
            for value in payload.values():  # createPerson / createCompany / record
                if isinstance(value, dict) and value.get("id"):
                    return value["id"]
    return None


def _extract_list(json_data):
    """Pull the record list out of a Twenty REST reply (array or {data:{plural:[...]}})."""
    payload = json_data.get("data") if isinstance(json_data, dict) else json_data
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list):
                return value
    return []


def _next_cursor(json_data):
    if isinstance(json_data, dict):
        page = json_data.get("pageInfo") or {}
        if page.get("hasNextPage") and page.get("endCursor"):
            return page["endCursor"]
    return None


def _is_connectivity(exc):
    return isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.SSLError,
        ),
    )


class nextcloudTwentyContactSyncPlugin(BaseSubscription):
    metadata = {
        "name": "nextcloudTwentyContactSyncPlugin",
        "description": (
            "Reconciles contacts between a Twenty CRM People and a Nextcloud "
            "Global address book, using AutoKB files as the source of truth. "
            "Writes corrections back to both sources over the network. "
            "Schedule this subscription on a cron interval."
        ),
        "sub_type": "SCHEDULED",
    }

    PROP_FIND_BODY = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:carddav">'
        "<d:prop><d:getetag/><c:address-data/></d:prop></d:propfind>"
    )

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "nextcloud_url": {
                    "type": "string",
                    "default": "http://nextcloud-web",
                    "description": "Base URL of the Nextcloud instance (default: in-cluster service)",
                },
                "nextcloud_username": {
                    "type": "string",
                    "minLength": 1,
                    "description": "Nextcloud admin user that owns the Global address book",
                },
                "nextcloud_password": {
                    "type": "string",
                    "minLength": 1,
                    "format": "password",
                    "description": "Password for the Nextcloud admin user",
                },
                "nextcloud_addressbook": {
                    "type": "string",
                    "default": "Global",
                    "description": "Address book name under the admin user's CardDAV",
                },
                "twenty_url": {
                    "type": "string",
                    "default": "http://twenty-app:3000",
                    "description": "Base URL of the Twenty CRM instance (default: in-cluster service)",
                },
                "twenty_api_key": {
                    "type": "string",
                    "minLength": 1,
                    "format": "password",
                    "description": "Twenty API key with read/write access to People",
                },
                "mode": {
                    "type": "string",
                    "enum": [
                        "Normal",
                        "Dry-Run",
                        "Full Reconciliation",
                        "Full Reconciliation Dry-Run",
                    ],
                    "default": "Normal",
                    "description": (
                        "Normal: sync as usual. "
                        "Dry-Run: compute against current AutoKB records and email a "
                        "report, no writes. "
                        "Full Reconciliation: delete all AutoKB records and fully "
                        "re-link and re-sync everything. "
                        "Full Reconciliation Dry-Run: act as if no AutoKB records exist "
                        "and email a report, no writes."
                    ),
                },
            },
            "required": [
                "nextcloud_url",
                "nextcloud_username",
                "nextcloud_password",
                "twenty_url",
                "twenty_api_key",
            ],
        }

    # ------------------------------------------------------------------ getData

    def getData(self, config, progress_callback):
        progress_callback(0, "Starting contact reconciliation run")
        nc_origin = config["nextcloud_url"].rstrip("/")
        nc_user = config["nextcloud_username"]
        nc_pass = config["nextcloud_password"]
        nc_book = config.get("nextcloud_addressbook") or "Global"
        nc_base = f"{nc_origin}/remote.php/dav/addressbooks/users/{nc_user}/{nc_book}"
        tw_url = config["twenty_url"].rstrip("/")
        tw_key = config["twenty_api_key"]
        mode = config.get("mode", "Normal")
        dry_run = mode in ("Dry-Run", "Full Reconciliation Dry-Run")
        full_reconcile = mode in ("Full Reconciliation", "Full Reconciliation Dry-Run")

        # 0. connectivity test (one email at most on a failed run)
        nc_reach = tw_reach = None
        try:
            self._probe_nextcloud(nc_origin, nc_user, nc_pass)
            self._ensure_addressbook(nc_base, nc_book, nc_user, nc_pass)
        except Exception as e:
            nc_reach = e
        try:
            self._probe_twenty(tw_url, tw_key)
        except Exception as e:
            tw_reach = e
        if nc_reach or tw_reach:
            body = ["Connectivity test failed for one or more sources:"]
            if nc_reach:
                body.append(f"- Nextcloud: {nc_reach}")
            if tw_reach:
                body.append(f"- Twenty: {tw_reach}")
            body.append("")
            body.append(
                "No reconciliation was performed and no changes were made. "
                "The subscription remains enabled for the next scheduled run."
            )
            send_smtp_notification(
                subject="[ContactSync] connectivity error", body="\n".join(body)
            )
            progress_callback(100, "Run aborted: connectivity error (emailed)")
            return

        # 1. load the AutoKB index (source of truth memory)
        out_dir = self.get_destination_path()
        if full_reconcile:
            if not dry_run and os.path.isdir(out_dir):
                # Full reconciliation: clear the source-of-truth memory on disk.
                for fname in os.listdir(out_dir):
                    if fname.endswith(".json"):
                        try:
                            os.remove(os.path.join(out_dir, fname))
                        except OSError:
                            pass
            # Both full modes start from an empty index (act as if no AutoKB exists).
            ak_by_key = {}
        else:
            ak_by_key = {}
            if os.path.isdir(out_dir):
                for fname in os.listdir(out_dir):
                    if not fname.endswith(".json"):
                        continue
                    try:
                        with open(os.path.join(out_dir, fname), "r") as f:
                            rec = json.load(f)
                    except (OSError, ValueError) as e:
                        self.log.warning("ak_file_unreadable", file=fname, error=str(e))
                        continue
                    rec["_ak_key"] = os.path.splitext(fname)[0]
                    ak_by_key[rec["_ak_key"]] = rec

        ak_by_href = {}
        ak_by_twenty = {}
        ak_by_email = {}
        for rec in ak_by_key.values():
            if rec.get("ncHref"):
                ak_by_href.setdefault(_href_path(rec["ncHref"]), rec)
            if rec.get("twentyId"):
                ak_by_twenty.setdefault(str(rec["twentyId"]), rec)
            if rec.get("email"):
                ak_by_email.setdefault(rec["email"].lower(), rec)

        progress_callback(10, f"Loaded {len(ak_by_key)} AutoKB records")

        # 2. fetch both sources (one email at most on a failed run)
        nc_fail = tw_fail = None
        nc_records = []
        tw_records = []
        try:
            nc_records = self._fetch_nextcloud(nc_origin, nc_base, nc_user, nc_pass)
        except Exception as e:
            nc_fail = e
        try:
            tw_records = self._fetch_twenty(tw_url, tw_key)
        except Exception as e:
            tw_fail = e

        if nc_fail or tw_fail:
            failing = []
            for name, exc in (("Nextcloud", nc_fail), ("Twenty", tw_fail)):
                if exc is not None:
                    failing.append((name, exc))
            if any(_is_connectivity(exc) for _, exc in failing):
                subject = "[ContactSync] connectivity error"
                head = "Connectivity test failed for one or more sources:"
            else:
                subject = "[ContactSync] error"
                head = "The contact sync run failed with an error:"
            body = [head]
            body.extend(f"- {name}: {exc}" for name, exc in failing)
            body.append("")
            body.append(
                "No reconciliation was performed and no changes were made. "
                "The subscription remains enabled for the next scheduled run."
            )
            send_smtp_notification(subject=subject, body="\n".join(body))
            progress_callback(100, "Run aborted (emailed)")
            return

        progress_callback(
            25,
            f"Fetched {len(nc_records)} Nextcloud and {len(tw_records)} Twenty contacts",
        )

        # 3. match source records to AutoKB persons (strong id match, email fallback)
        matched_nc = {}
        matched_tw = {}
        unmatched_nc = []
        unmatched_tw = []
        abnormals = []

        for rec in nc_records:
            target = ak_by_href.get(rec["href"])
            via_email = False
            if target is None and rec["email"]:
                target = ak_by_email.get(rec["email"])
                via_email = True
            if target is None:
                unmatched_nc.append(rec)
                continue
            if via_email and target.get("ncHref") and _href_path(target["ncHref"]) != rec["href"]:
                abnormals.append(
                    f"possible duplicate Nextcloud card for {_display(target)} "
                    f"(new href {rec['href']}, known {target['ncHref']})"
                )
                continue
            key = target["_ak_key"]
            if key in matched_nc:
                abnormals.append(
                    f"multiple Nextcloud cards matched to same person {_display(target)}; "
                    f"skipping {rec['href']}"
                )
                continue
            if via_email and not target.get("ncHref"):
                target["ncHref"] = rec["href"]
            matched_nc[key] = rec

        for rec in tw_records:
            target = ak_by_twenty.get(str(rec["tw_id"]))
            via_email = False
            if target is None and rec["email"]:
                target = ak_by_email.get(rec["email"])
                via_email = True
            if target is None:
                unmatched_tw.append(rec)
                continue
            if via_email and target.get("twentyId") and str(target["twentyId"]) != str(rec["tw_id"]):
                abnormals.append(
                    f"possible duplicate Twenty person for {_display(target)} "
                    f"(new id {rec['tw_id']}, known {target['twentyId']})"
                )
                continue
            key = target["_ak_key"]
            if key in matched_tw:
                abnormals.append(
                    f"multiple Twenty people matched to same person {_display(target)}; "
                    f"skipping {rec['tw_id']}"
                )
                continue
            if via_email and not target.get("twentyId"):
                target["twentyId"] = rec["tw_id"]
            matched_tw[key] = rec

        # 4. index unmatched records by email for the happy-path cross-pairing
        nc_by_email = {}
        tw_by_email = {}
        for rec in unmatched_nc:
            if rec["email"]:
                nc_by_email.setdefault(rec["email"], []).append(rec)
        for rec in unmatched_tw:
            if rec["email"]:
                tw_by_email.setdefault(rec["email"], []).append(rec)

        paired_emails = set()
        for email in set(nc_by_email) & set(tw_by_email):
            n_list = nc_by_email[email]
            t_list = tw_by_email[email]
            if len(n_list) == 1 and len(t_list) == 1:
                paired_emails.add(email)
            else:
                abnormals.append(
                    f"ambiguous email pairing for <{email}>: "
                    f"{len(n_list)} Nextcloud / {len(t_list)} Twenty candidates"
                )

        progress_callback(40, "Sources matched to AutoKB records")

        # 5. reconcile
        planned = []
        deletions = []
        keys_to_delete = []
        done = 0

        # 5a. all persons known to AutoKB (matched on either side, or absent from both)
        matched_keys = sorted(set(matched_nc) | set(matched_tw) | set(ak_by_key))
        for ak_key in matched_keys:
            ak = ak_by_key[ak_key]
            nc_side = matched_nc.get(ak_key)
            tw_side = matched_tw.get(ak_key)
            label = _display(ak)

            if nc_side is None and tw_side is None:
                planned.append(f"DROP  {label}: deleted in both sources")
                if not dry_run:
                    keys_to_delete.append(ak_key)
            elif nc_side is not None and tw_side is None:
                deletions.append(f"{label}: missing in Twenty - delete this contact in Nextcloud to confirm")
                planned.append(f"REPORT {label}: missing in Twenty - delete this contact in Nextcloud to confirm")
            elif nc_side is None and tw_side is not None:
                deletions.append(f"{label}: missing in Nextcloud - delete this contact in Twenty to confirm")
                planned.append(f"REPORT {label}: missing in Nextcloud - delete this contact in Twenty to confirm")
            else:
                # company is handled as its own dimension (Twenty owns companies)
                eff_id, eff_name, company_action = self._company_state(
                    tw_url, tw_key, nc_side, tw_side, ak, dry_run, abnormals, label
                )
                nc_org = _clean(nc_side.get("company"))
                nc_needs = (nc_org or "") != (eff_name or "")
                tw_needs = (tw_side.get("companyId") or None) != eff_id

                # other fields still follow the existing NC-wins / from-Twenty rules
                nc_other_same = _same_other(nc_side, ak)
                tw_other_same = _same_other(tw_side, ak)
                write_nc_other = nc_other_same and not tw_other_same
                write_tw_other = not nc_other_same

                write_nc = write_nc_other or nc_needs
                write_tw = write_tw_other or tw_needs

                if not write_nc and not write_tw:
                    planned.append(f"SAME  {label}: no change")
                else:
                    if nc_other_same and not tw_other_same:
                        target = _clone_compare(tw_side)  # Twenty changed
                    else:
                        target = _clone_compare(nc_side)  # NC wins on other fields
                    target["company"] = eff_name or ""
                    target["companyId"] = eff_id
                    target["twentyId"] = ak.get("twentyId")
                    target["ncHref"] = ak.get("ncHref")
                    if company_action == "seed":
                        planned.append(f"UPDATE {label}: seed company into Twenty")
                    elif company_action == "remove":
                        planned.append(f"UPDATE {label}: company removed (from Twenty)")
                    elif nc_needs and not write_tw_other and not tw_needs:
                        planned.append(f"UPDATE {label}: company from Twenty")
                    else:
                        planned.append(f"UPDATE {label}: Nextcloud wins")
                    if not dry_run:
                        self._apply(
                            target, nc_side, tw_side, out_dir,
                            nc_base, nc_origin, nc_user, nc_pass, tw_url, tw_key,
                            abnormals, write_nc=write_nc, write_tw=write_tw,
                        )

            done += 1
            progress_callback(40 + int(45 * done / max(1, len(matched_keys))))

        # 5b. happy-path pairs (both sources already have the same-email person)
        for email in sorted(paired_emails):
            n_rec = nc_by_email[email][0]
            t_rec = tw_by_email[email][0]
            target = _clone_compare(n_rec)
            target["ncHref"] = n_rec["href"]
            target["twentyId"] = t_rec["tw_id"]
            eff_id, eff_name, _ = self._company_state(
                tw_url, tw_key, n_rec, t_rec, None, dry_run, abnormals, _display(target)
            )
            target["company"] = eff_name or ""
            target["companyId"] = eff_id
            planned.append(f"LINK  {_display(target)}: paired by email, seed from Nextcloud")
            if not dry_run:
                self._persist_ak(target, out_dir, abnormals, _display(target))

        # 5c. newcomers present in only one source
        for rec in unmatched_nc:
            if rec["email"] in paired_emails:
                continue
            target = _clone_compare(rec)
            target["ncHref"] = rec["href"]
            eff_id, eff_name, _ = self._company_state(
                tw_url, tw_key, rec, None, None, dry_run, abnormals, _display(target)
            )
            target["company"] = eff_name or ""
            target["companyId"] = eff_id
            planned.append(f"NEW   {_display(target)}: create in Twenty")
            if not dry_run:
                self._apply(
                    target, rec, None, out_dir, nc_base, nc_origin, nc_user,
                    nc_pass, tw_url, tw_key, abnormals, write_nc=False, write_tw=True,
                )

        for rec in unmatched_tw:
            if rec["email"] in paired_emails:
                continue
            target = _clone_compare(rec)
            target["twentyId"] = rec["tw_id"]
            eff_id, eff_name, _ = self._company_state(
                tw_url, tw_key, None, rec, None, dry_run, abnormals, _display(target)
            )
            target["company"] = eff_name or ""
            target["companyId"] = eff_id
            planned.append(f"NEW   {_display(target)}: create in Nextcloud")
            if not dry_run:
                self._apply(
                    target, None, rec, out_dir, nc_base, nc_origin, nc_user,
                    nc_pass, tw_url, tw_key, abnormals, write_nc=True, write_tw=False,
                )

        # 5d. remove AutoKB files for persons deleted in both sources
        if not dry_run:
            for key in keys_to_delete:
                self._delete_ak(key, out_dir)

        # 6. report
        actions = [p for p in planned if not p.startswith(("SAME  ", "REPORT "))]
        if dry_run:
            body = ["DRY RUN - no changes were written.", ""]
            if full_reconcile:
                body.insert(1, "Mode: Full Reconciliation Dry-Run - acting as if all AutoKB records are absent.")
                body.insert(2, "")
            body.extend(planned)
            if deletions:
                body.append("")
                body.append("Pending deletions (would be reported):")
                body.extend(f"  - {d}" for d in deletions)
            if abnormals:
                body.append("")
                body.append("Abnormal conditions (not acted on):")
                body.extend(f"  - {a}" for a in abnormals)
            body.append("")
            body.append("This is a preview only; nothing was sent or modified.")
            send_smtp_notification(
                subject=f"[ContactSync] dry-run report ({len(actions)} actions, {len(abnormals)} abnormal)",
                body="\n".join(body),
            )
            progress_callback(
                100,
                f"Dry run: {len(actions)} actions, {len(deletions)} deletions reported, {len(abnormals)} abnormal (emailed)",
            )
            return

        if deletions or abnormals:
            head = []
            if deletions:
                head.append(f"Pending deletions ({len(deletions)}):")
                head.append("Missing from exactly ONE source. Auto-delete is disabled -")
                head.append("delete these manually in BOTH places:")
                head.append("")
                head.extend(f"  - {d}" for d in deletions)
                head.append("")
            if abnormals:
                head.append(f"Abnormal conditions ({len(abnormals)}) - not acted on:")
                head.append("")
                head.extend(f"  - {a}" for a in abnormals)
                head.append("")
            head.append("All other reconciled changes were applied normally this run.")
            send_smtp_notification(
                subject=f"[ContactSync] {len(deletions)} deletions / {len(abnormals)} abnormal",
                body="\n".join(head),
            )

        progress_callback(
            100,
            f"Done: {len(actions)} actions, {len(deletions)} deletions reported, {len(abnormals)} abnormal",
        )

    # --------------------------------------------------------------- helpers

    def _delete_ak(self, key, out_dir):
        try:
            os.remove(os.path.join(out_dir, f"{key}.json"))
            self.log.info("ak_record_deleted", key=key)
        except FileNotFoundError:
            pass

    def _persist_ak(self, target, out_dir, abnormals, label):
        record = _clone_compare(target)
        record["twentyId"] = target.get("twentyId")
        record["ncHref"] = target.get("ncHref")
        # the IDs are the strong cross-source link; never write a record without both
        if not record.get("twentyId") or not record.get("ncHref"):
            abnormals.append(
                f"AK record for {label} missing a source id and was NOT written "
                f"(twentyId={record.get('twentyId')!r}, ncHref={record.get('ncHref')!r}); manual review needed"
            )
            self.log.error(
                "ak_persist_skipped", label=label,
                twentyId=record.get("twentyId"), ncHref=record.get("ncHref"),
            )
            return None
        new_key = _ak_key(record)
        tmp = f"/tmp/contactsync_{new_key}.json"
        with open(tmp, "w") as f:
            json.dump(record, f)
        self.move_to_destination(tmp)
        return new_key

    def _apply(self, target, nc_side, tw_side, out_dir, nc_base, nc_origin,
               nc_user, nc_pass, tw_url, tw_key, abnormals, write_nc, write_tw):
        try:
            if write_tw:
                company_id, company_name, tw_id = self._write_twenty(
                    tw_url, tw_key, target, tw_side
                )
                target["companyId"] = company_id
                if company_name:
                    target["company"] = company_name
                if tw_id:
                    target["twentyId"] = tw_id
                elif not (tw_side and tw_side.get("tw_id")):
                    raise ValueError(
                        f"created Twenty person but could not capture its id for {_display(target)}"
                    )
            if write_nc:
                href = self._write_nextcloud(target, nc_side, nc_base, nc_origin, nc_user, nc_pass)
                if href:
                    target["ncHref"] = _href_path(href)
                elif not nc_side:
                    raise ValueError(
                        f"created Nextcloud card but could not capture its href for {_display(target)}"
                    )
            self._persist_ak(target, out_dir, abnormals, _display(target))
        except SubscriptionCancelledError:
            raise
        except Exception as e:
            self.log.error("record_failed", error=str(e))
            abnormals.append(f"write failed for {_display(target)}: {e}")

    # --- connectivity probes (non-mutating)
    def _probe_nextcloud(self, nc_origin, nc_user, nc_pass):
        resp = requests.get(
            f"{nc_origin}/remote.php/dav", auth=(nc_user, nc_pass), timeout=10
        )
        resp.raise_for_status()

    def _ensure_addressbook(self, nc_base, nc_book, nc_user, nc_pass):
        """Idempotent: create the configured address book if it does not exist."""
        collection = f"{nc_base}/"
        headers = {"Content-Type": "application/xml; charset=UTF-8"}
        exists = requests.request(
            "PROPFIND", collection, auth=(nc_user, nc_pass), timeout=30,
            data=self.PROP_FIND_BODY, headers={"Depth": "0", **headers},
        )
        if exists.status_code == 207 and _is_addressbook(exists.text):
            return True
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<d:mkcol xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">'
            "<d:set><d:prop>"
            "<d:resourcetype><d:collection/><card:addressbook/></d:resourcetype>"
            f"<d:displayname>{_esc(nc_book)}</d:displayname>"
            "</d:prop></d:set></d:mkcol>"
        )
        created = requests.request(
            "MKCOL", collection, auth=(nc_user, nc_pass), timeout=30,
            data=body, headers=headers,
        )
        if created.status_code in (201, 207, 405):
            return True
        created.raise_for_status()
        return False

    def _probe_twenty(self, tw_url, tw_key):
        resp = requests.get(
            f"{tw_url}/rest/people",
            params={"limit": 1},
            headers={"Authorization": f"Bearer {tw_key}"},
            timeout=10,
        )
        resp.raise_for_status()

    # --- Nextcloud CardDAV
    def _fetch_nextcloud(self, nc_origin, nc_base, nc_user, nc_pass):
        result = []
        resp = requests.request(
            "PROPFIND", nc_base, auth=(nc_user, nc_pass),
            data=self.PROP_FIND_BODY, timeout=30,
            headers={"Depth": "1", "Content-Type": "application/xml; charset=utf-8"},
        )
        resp.raise_for_status()
        for href, etag, card_data in _multistatus_cards(resp.text):
            if not card_data:
                card_resp = requests.get(
                    _abs_url(nc_origin, href), auth=(nc_user, nc_pass), timeout=30
                )
                card_resp.raise_for_status()
                card_data = card_resp.text
            rec = _parse_vcard(card_data)
            if not rec["email"]:
                continue
            rec["href"] = href
            rec["etag"] = etag
            result.append(rec)
        return result

    def _write_nextcloud(self, record, nc_side, nc_base, nc_origin, nc_user, nc_pass):
        card = _serialize_vcard(record, uuid.uuid4().hex)
        headers = {"Content-Type": "text/vcard"}
        if nc_side and nc_side.get("href"):
            href = _abs_url(nc_origin, nc_side["href"])
            if nc_side.get("etag"):
                headers["If-Match"] = nc_side["etag"]
        else:
            href = f"{nc_base}/{uuid.uuid4().hex}.vcf"
        resp = requests.put(
            href, data=card.encode("utf-8"), auth=(nc_user, nc_pass),
            headers=headers, timeout=30,
        )
        resp.raise_for_status()
        return href

    # --- Twenty REST
    def _fetch_twenty(self, tw_url, tw_key):
        headers = {"Authorization": f"Bearer {tw_key}"}
        result = []
        starting_after = None
        for _ in range(50):
            params = {"limit": 1000}
            if starting_after:
                params["starting_after"] = starting_after
            resp = requests.get(
                f"{tw_url}/rest/people", headers=headers, params=params, timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            records = _extract_list(data)
            for p in records:
                rec = self._twenty_to_record(p)
                if rec["email"]:
                    result.append(rec)
            cursor = _next_cursor(data)
            if not cursor or not records:
                break
            starting_after = cursor
        return result

    @staticmethod
    def _twenty_to_record(p):
        name = p.get("name")
        if isinstance(name, dict):
            first = _clean(name.get("firstName"))
            last = _clean(name.get("lastName"))
        elif isinstance(name, str):
            parts = name.strip().split(" ", 1)
            first, last = parts[0], (parts[1] if len(parts) > 1 else "")
        else:
            first, last = "", ""

        emails = p.get("emails")
        email = _clean(emails.get("primaryEmail") if isinstance(emails, dict) else emails).lower()

        phones = p.get("phones")
        if isinstance(phones, dict):
            phone = _canonical_phone(
                _clean(phones.get("primaryPhoneCallingCode"))
                + _clean(phones.get("primaryPhoneNumber"))
            )
        else:
            phone = _canonical_phone(phones)

        company = ""
        company_id = p.get("companyId")
        comp = p.get("company")
        if isinstance(comp, dict):
            company = _clean(comp.get("name"))
            if comp.get("id"):
                company_id = comp["id"]
        elif isinstance(comp, str):
            company = _clean(comp)

        return {
            "firstName": first,
            "lastName": last,
            "email": email,
            "phone": phone,
            "jobTitle": _clean(p.get("jobTitle")),
            "company": company,
            "companyId": company_id,
            "tw_id": p.get("id"),
        }

    def _write_twenty(self, tw_url, tw_key, record, tw_side):
        preferred_id = record.get("companyId")
        if not preferred_id and tw_side:
            preferred_id = tw_side.get("companyId")
        company_id, company_name = self._resolve_company(
            tw_url, tw_key, preferred_id, record.get("company")
        )
        payload = {
            "name": {
                "firstName": record.get("firstName"),
                "lastName": record.get("lastName"),
            },
            "emails": {"primaryEmail": record.get("email")},
            "phones": _split_phone(record.get("phone")),
            "jobTitle": record.get("jobTitle"),
            "companyId": company_id,
        }
        headers = {"Authorization": f"Bearer {tw_key}"}
        if tw_side and tw_side.get("tw_id"):
            resp = requests.patch(
                f"{tw_url}/rest/people/{tw_side['tw_id']}", headers=headers,
                json=payload, timeout=30,
            )
            resp.raise_for_status()
            tw_id = tw_side["tw_id"]
        else:
            resp = requests.post(
                f"{tw_url}/rest/people", headers=headers, json=payload, timeout=30,
            )
            resp.raise_for_status()
            tw_id = _extract_id(resp.json())
        return company_id, company_name, tw_id

    def _resolve_company(self, tw_url, tw_key, preferred_id, company_name):
        headers = {"Authorization": f"Bearer {tw_key}"}
        companies = self._load_companies(tw_url, tw_key)
        by_name = {}
        for cid, cname in companies.items():
            by_name.setdefault(cname.lower(), cid)
        if preferred_id:
            cname = companies.get(preferred_id)
            return preferred_id, cname or company_name
        name = _clean(company_name)
        if not name:
            return None, ""
        cid = by_name.get(name.lower())
        if cid is None:
            resp = requests.post(
                f"{tw_url}/rest/companies", headers=headers,
                json={"name": name}, timeout=30,
            )
            resp.raise_for_status()
            cid = _extract_id(resp.json())
        return cid, companies.get(cid) or name

    def _load_companies(self, tw_url, tw_key):
        headers = {"Authorization": f"Bearer {tw_key}"}
        companies = {}
        starting_after = None
        for _ in range(50):
            params = {"limit": 1000}
            if starting_after:
                params["starting_after"] = starting_after
            resp = requests.get(
                f"{tw_url}/rest/companies", headers=headers, params=params, timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            records = _extract_list(data)
            for c in records:
                cid = c.get("id")
                if cid:
                    companies[cid] = _clean(c.get("name"))
            cursor = _next_cursor(data)
            if not cursor or not records:
                break
            starting_after = cursor
        return companies

    def _company_map(self, tw_url, tw_key):
        if getattr(self, "_company_map_cache", None) is None:
            self._company_map_cache = self._load_companies(tw_url, tw_key)
        return self._company_map_cache

    def _create_company(self, tw_url, tw_key, name, names):
        headers = {"Authorization": f"Bearer {tw_key}"}
        resp = requests.post(
            f"{tw_url}/rest/companies", headers=headers,
            json={"name": name}, timeout=30,
        )
        resp.raise_for_status()
        cid = _extract_id(resp.json())
        if cid:
            names[cid] = name
        return cid

    def _company_state(self, tw_url, tw_key, nc_side, tw_side, ak, dry_run, abnormals, label):
        """Determine the effective company linkage: (eff_id, eff_name, action)."""
        ak_id = (ak or {}).get("companyId")
        tw_id = (tw_side or {}).get("companyId")
        tw_raw = (tw_side or {}).get("company") or ""
        nc_org = (nc_side or {}).get("company") or ""

        if tw_id:
            # Twenty owns the linkage -> mirror its resolved name
            names = self._company_map(tw_url, tw_key)
            return tw_id, names.get(tw_id) or tw_raw or "", "mirror"

        if ak_id:
            # AK knew the link but Twenty cleared it -> deliberate removal
            return None, "", "remove"

        if nc_org:
            # NC ORG with no known link -> seed a Twenty company
            names = self._company_map(tw_url, tw_key)
            by_name = {}
            for cid, cname in names.items():
                by_name.setdefault(cname.strip().lower(), []).append(cid)
            hits = by_name.get(nc_org.strip().lower(), [])
            if len(hits) == 1:
                return hits[0], names[hits[0]] or nc_org, "seed"
            if len(hits) > 1:
                abnormals.append(f"ambiguous Twenty companies for ORG '{nc_org}' on {label}; not linking")
                return None, nc_org, "none"
            if dry_run:
                return None, nc_org, "seed"
            cid = self._create_company(tw_url, tw_key, nc_org, names)
            if cid:
                return cid, nc_org, "seed"
            abnormals.append(f"could not create Twenty company for ORG '{nc_org}' on {label}")
            return None, nc_org, "none"

        return None, "", "none"
