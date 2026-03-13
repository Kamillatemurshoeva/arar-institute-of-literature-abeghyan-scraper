#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://arar.sci.am"

START_URL = "https://arar.sci.am/dlibra/results?q=&action=SimpleSearchAction&type=-6&p=0&qf1=collections:16"
LIST_URL = "https://arar.sci.am/dlibra/results?q=&action=SimpleSearchAction&type=-6&p={}&qf1=collections:16"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en,hy;q=0.9,ru;q=0.8",
}

DELAY = 1.2
MAX_PAGES = 300


def make_session():

    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(HEADERS)

    return session


def clean(text):

    if not text:
        return ""

    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def get_html(session, url, retries=3):

    for attempt in range(1, retries + 1):

        try:

            time.sleep(DELAY)

            r = session.get(url, timeout=(20, 90))

            r.raise_for_status()

            return r.text

        except requests.exceptions.RequestException:

            print(f"REQUEST FAILED ({attempt}/{retries}): {url}")

            time.sleep(2 * attempt)

    return None


def get_ids(url):

    m = re.search(r"/publication/(\d+)/edition/(\d+)", url)

    if m:
        return m.group(1), m.group(2)

    return "", ""


def get_item_links(html):

    soup = BeautifulSoup(html, "html.parser")

    links = []

    for a in soup.find_all("a", href=True):

        href = a["href"]

        if "/dlibra/publication/" in href and "/edition/" in href:

            links.append(urljoin(BASE_URL, href))

    return list(dict.fromkeys(links))


def collect_links(session):

    all_links = []

    seen = set()

    no_new_pages = 0

    for page in range(MAX_PAGES):

        url = LIST_URL.format(page)

        html = get_html(session, url)

        if not html:
            continue

        links = get_item_links(html)

        new_count = 0

        for link in links:

            pub_id, ed_id = get_ids(link)

            key = ed_id or pub_id or link

            if key not in seen:

                seen.add(key)

                all_links.append(link)

                new_count += 1

        print(f"Page {page}: +{new_count} links (total: {len(all_links)})")

        if new_count == 0:
            no_new_pages += 1
        else:
            no_new_pages = 0

        if no_new_pages >= 3:
            break

    return all_links


def get_text_lines(html):

    soup = BeautifulSoup(html, "html.parser")

    text = soup.get_text("\n")

    lines = [clean(x) for x in text.split("\n")]

    return [x for x in lines if x]


def slice_object_lines(lines):

    start = None

    for i, line in enumerate(lines):

        if line == "Object":

            start = i

            break

    if start is None:
        return lines

    result = []

    for line in lines[start:]:

        if line in {"Recently viewed", "Objects", "Collections", "Similar"} and len(result) > 10:

            break

        result.append(line)

    return result


def build_field_map(lines):

    fields = {}

    current_key = None

    current_value = []

    for line in lines:

        if ":" in line and not line.endswith(":"):

            left, right = line.split(":", 1)

            left = clean(left)

            right = clean(right)

            if left and right:

                fields[left] = right

                current_key = None
                current_value = []

                continue

        m = re.match(r"^(.+):$", line)

        if m:

            if current_key:

                fields[current_key] = clean(" ".join(current_value))

            current_key = m.group(1)

            current_value = []

            continue

        if current_key:

            current_value.append(line)

    if current_key:

        fields[current_key] = clean(" ".join(current_value))

    return fields


def pick_field(fields, names):

    for name in names:

        if name in fields:

            return fields[name]

    return ""


def extract_year(text):

    if not text:
        return None

    m = re.search(r"\b(1[0-9]{3}|20[0-9]{2})\b", text)

    if m:
        return int(m.group(1))

    return None


def parse_item(session, url):

    html = get_html(session, url)

    if not html:
        return None

    lines = get_text_lines(html)

    object_lines = slice_object_lines(lines)

    fields = build_field_map(object_lines)

    pub_id, ed_id = get_ids(url)

    title = pick_field(fields, ["Title", "Վերնագիր", "Заглавие", "Название"])
    creator = pick_field(fields, ["Creator", "Author", "Ստեղծող", "Հեղինակ"])
    date = pick_field(fields, ["Date", "Date of publication"])
    place = pick_field(fields, ["Place of publishing", "Place"])
    publisher = pick_field(fields, ["Publisher"])
    language = pick_field(fields, ["Language"])
    subjects = pick_field(fields, ["Subjects", "Subject and keywords"])

    year = extract_year(date)

    record = {

        "id": ed_id or pub_id or url,
        "publication_id": pub_id,
        "edition_id": ed_id,
        "title": title,
        "author_creator": creator,
        "date_period": date,
        "year": year,
        "place_of_publishing": place,
        "publisher": publisher,
        "language": language,
        "subject_keywords": subjects,
        "url_original_object": url,
    }

    return record


def save_csv(records, filename):

    if not records:
        return

    with open(filename, "w", encoding="utf-8-sig", newline="") as f:

        writer = csv.DictWriter(f, fieldnames=records[0].keys())

        writer.writeheader()

        writer.writerows(records)


def save_jsonl(records, filename):

    if not records:
        return

    with open(filename, "w", encoding="utf-8") as f:

        for r in records:

            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():

    session = make_session()

    print("Collecting links...")

    links = collect_links(session)

    print(f"\nTotal links: {len(links)}")

    records = []

    for url in tqdm(links, desc="Scraping"):

        record = parse_item(session, url)

        if record:
            records.append(record)

    save_csv(records, "arar_collection_16.csv")

    save_jsonl(records, "arar_collection_16.jsonl")

    print(f"\nSaved {len(records)} records")


if __name__ == "__main__":

    main()