import os

from airflow.sdk import dag, task
import pendulum


@dag(
    dag_id="daily_zen_newsletter",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
)
def daily_zen_newsletter():

    # ============================================================
    # 1. GET FRESH QUOTES
    # ============================================================

    @task
    def raw_zen_quotes() -> list[dict]:
        """
        Gets a fresh set of quotes from ZenQuotes every time
        the task runs.
        """

        import requests
        import time

        # Cache-busting parameter.
        # This makes each request unique.
        cache_buster = str(time.time_ns())

        response = requests.get(
            "https://zenquotes.io/api/quotes/random",
            params={
                "_": cache_buster
            },
            timeout=30,
        )

        response.raise_for_status()

        quotes = response.json()

        print("========================================")
        print("FRESH QUOTES RECEIVED FROM ZENQUOTES")
        print("========================================")

        for quote in quotes:
            print(
                f'{quote["q"]} - {quote["a"]} '
                f'({quote["c"]} characters)'
            )

        if not quotes:
            raise ValueError(
                "ZenQuotes returned no quotes."
            )

        return quotes


    # ============================================================
    # 2. SELECT QUOTES
    # ============================================================

    @task
    def selected_quotes(quotes: list[dict]) -> dict:
        """
        Selects short, median and long quotes.
        """

        import numpy as np

        print("========================================")
        print("QUOTES RECEIVED BY selected_quotes")
        print("========================================")
        print(quotes)

        if not quotes:
            raise ValueError(
                "selected_quotes received no quotes."
            )

        if len(quotes) < 3:
            raise ValueError(
                f"Need at least 3 quotes, "
                f"but received {len(quotes)}."
            )

        quote_character_counts = [
            int(quote["c"])
            for quote in quotes
        ]

        median = np.median(
            quote_character_counts
        )

        # Find quote closest to median
        median_quote = min(
            quotes,
            key=lambda quote: abs(
                int(quote["c"]) - median
            ),
        )

        # Remove median quote
        remaining_quotes = [
            quote
            for quote in quotes
            if quote is not median_quote
        ]

        # Shortest quote
        short_quote = min(
            remaining_quotes,
            key=lambda quote: int(quote["c"]),
        )

        # Remove short quote
        remaining_quotes = [
            quote
            for quote in remaining_quotes
            if quote is not short_quote
        ]

        # Longest quote
        long_quote = max(
            remaining_quotes,
            key=lambda quote: int(quote["c"]),
        )

        selected = {
            "short_q": short_quote,
            "median_q": median_quote,
            "long_q": long_quote,
        }

        print("========================================")
        print("SELECTED QUOTES")
        print("========================================")
        print(selected)

        return selected


    # ============================================================
    # 3. FORMAT NEWSLETTER
    # ============================================================

    @task
    def formatted_newsletter(
        selected: dict,
        context=None,
    ) -> str:
        """
        Formats the newsletter using newsletter_template.txt.
        """

        from airflow.sdk import ObjectStoragePath

        OBJECT_STORAGE_SYSTEM = os.getenv(
            "OBJECT_STORAGE_SYSTEM",
            "file",
        )

        OBJECT_STORAGE_CONN_ID = os.getenv(
            "OBJECT_STORAGE_CONN_ID",
            None,
        )

        OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv(
            "OBJECT_STORAGE_PATH_NEWSLETTER",
            "include/newsletter",
        )

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://"
            f"{OBJECT_STORAGE_PATH_NEWSLETTER}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )

        # Use today's actual date.
        date = pendulum.now("UTC").strftime(
            "%Y-%m-%d"
        )

        newsletter_template_path = (
            object_storage_path
            / "newsletter_template.txt"
        )

        newsletter_template = (
            newsletter_template_path.read_text()
        )

        # Fill template with the NEW quotes
        newsletter = newsletter_template.format(
            quote_text_1=selected["short_q"]["q"],
            quote_author_1=selected["short_q"]["a"],

            quote_text_2=selected["median_q"]["q"],
            quote_author_2=selected["median_q"]["a"],

            quote_text_3=selected["long_q"]["q"],
            quote_author_3=selected["long_q"]["a"],

            date=date,
        )

        # Save newsletter
        date_newsletter_path = (
            object_storage_path
            / f"{date}_newsletter.txt"
        )

        date_newsletter_path.write_text(
            newsletter
        )

        print("========================================")
        print("FORMATTED NEWSLETTER")
        print("========================================")
        print(newsletter)

        return newsletter


    # ============================================================
    # 4. SEND EMAIL AS A NEW EMAIL
    # ============================================================

    @task
    def send_newsletter(newsletter: str) -> None:
        """
        Sends the newsletter as a new standalone email.
        """

        import uuid

        from airflow.providers.smtp.hooks.smtp import (
            SmtpHook
        )

        print("========================================")
        print("NEWSLETTER RECEIVED BY send_newsletter")
        print("========================================")
        print(newsletter)

        if not newsletter:
            raise ValueError(
                "send_newsletter received no newsletter."
            )

        # --------------------------------------------------------
        # Email recipients
        # --------------------------------------------------------

        recipient_string = os.environ.get(
            "NEWSLETTER_RECIPIENT"
        )

        if not recipient_string:
            raise ValueError(
                "NEWSLETTER_RECIPIENT is not configured."
            )

        recipients = [
            email.strip()
            for email in recipient_string.split(",")
            if email.strip()
        ]

        print("EMAIL RECIPIENTS:")
        print(recipients)

        # --------------------------------------------------------
        # Create a unique subject for every newsletter
        # --------------------------------------------------------

        today = pendulum.now("UTC").strftime(
            "%Y-%m-%d"
        )

        subject = (
            f"Daily Reality Tunnel - {today}"
        )

        # --------------------------------------------------------
        # Unique Message-ID
        # --------------------------------------------------------

        message_id = (
            f"<daily-reality-tunnel-"
            f"{uuid.uuid4()}@newsletter>"
        )

        print("EMAIL SUBJECT:")
        print(subject)

        print("MESSAGE ID:")
        print(message_id)

        # --------------------------------------------------------
        # Send email
        # --------------------------------------------------------

        with SmtpHook(
            smtp_conn_id="smtp_default"
        ) as hook:

            hook.send_email_smtp(
                to=recipients,
                subject=subject,
                html_content=newsletter.replace(
                    "\n",
                    "<br>",
                ),
                custom_headers={
                    "Message-ID": message_id,
                },
            )

        print("========================================")
        print(
            f"NEW NEWSLETTER EMAIL SENT TO: "
            f"{recipients}"
        )
        print("========================================")


    # ============================================================
    # PIPELINE
    # ============================================================

    quotes = raw_zen_quotes()

    selected = selected_quotes(quotes)

    newsletter = formatted_newsletter(selected)

    send_newsletter(newsletter)


# ================================================================
# CREATE DAG
# ================================================================

daily_zen_newsletter()