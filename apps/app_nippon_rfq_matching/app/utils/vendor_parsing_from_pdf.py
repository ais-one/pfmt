import re

import pdfplumber


def extract_vendor_name(path):
    with pdfplumber.open(path) as pdf:
        text = pdf.pages[0].extract_text() or ""

        # normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()

        # =========================
        # 1. SHIPSERV FORMAT (OLD)
        # =========================
        pattern_shipsserv = (
            r"SHIPSERV\s+BUYER\s+RECORD.*?"
            r"SHIPSERV\s+SUPPLIER\s+RECORD\s+"
            r"(.+?)\s*\(\d+\)\s+"
            r"(.+?)\s*\(\d+\)"
        )

        match = re.search(pattern_shipsserv, text, re.IGNORECASE)

        if match:
            return {
                "buyer": match.group(1).strip(),
                "vendor": match.group(2).strip(),
                "source": "shipsserv",
            }

        # =========================
        # 2. RFQ FORMAT (NEW)
        # =========================
        block_match = re.search(
            r"Buyer Details:\s*Supplier Details:\s*(.*?)\s*RFQ Ref:",
            text,
            re.IGNORECASE,
        )

        if block_match:
            block = block_match.group(1)

            companies = re.findall(r"([A-Za-z0-9\-\(\)\.,& ]+?)\s*\(.*?TNID", block)

            if len(companies) >= 2:
                return {
                    "buyer": companies[0].strip(),
                    "vendor": companies[1].strip(),
                    "source": "rfq",
                }

        # 3. SIMPLE FORMAT (NEW)
        pattern_simple = re.search(
            r"Buyer\s+Supplier\s+Company Name:\s*(.*?)\s+Name:\s*(.*?)\s+Address:",
            text,
            re.IGNORECASE,
        )

        if pattern_simple:
            return {
                "buyer": pattern_simple.group(1).strip(),
                "vendor": pattern_simple.group(2).strip(),
                "source": "simple_format",
            }

        # =========================
        # 4. FALLBACK (LAST PAGE)
        # =========================
        fallback = re.search(
            r"Sent from\s+(.+?)\s*\(\d+\)\s+To\s+(.+?)\s*\(\d+\)", text, re.IGNORECASE
        )

        if fallback:
            return {
                "buyer": fallback.group(1).strip(),
                "vendor": fallback.group(2).strip(),
                "source": "fallback",
            }

    return {}
