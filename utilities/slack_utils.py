#!/usr/bin/env python3
'''
Slack Notification Utilities
============================

Sends Slack notifications for tool sync error reporting via the
email-based Slack API at https://slack-api.subtr.joby.aero.
Uses Slack Block Kit for rich message formatting.

Created: 2026-04-15
Author: Jae Osenbach
'''

import json
import requests
from typing import Dict, Any

from utilities.shared_sync_utils import log_and_print


def send_slack_notification(slack_config: Dict[str, str], stats: Dict[str, Any]) -> bool:
    """Post an error summary to Slack via the email-based Slack API.

    Parameters
    ----------
    slack_config : dict
        Must contain ``api_url`` and ``recipient_email``.
    stats : dict
        Sync statistics including error counts and details.

    Returns True if the message was accepted, False otherwise.
    Never raises — all exceptions are caught and logged so the sync
    pipeline is never interrupted by a notification failure.
    """
    try:
        api_url = slack_config.get('api_url', '')
        email = slack_config.get('recipient_email', '')
        if not api_url or not email:
            log_and_print("Slack config missing api_url or recipient_email — skipping notification", "warning")
            return False

        blocks = _build_blocks(stats)
        error_count = stats.get("errors", 0)

        payload = {
            "email": email,
            "message": json.dumps(blocks),
            "alt_text": f"Tool Sync completed with {error_count} error{'s' if error_count != 1 else ''}",
        }

        resp = requests.post(
            api_url,
            json=payload,
            timeout=10,
        )

        if resp.status_code != 200:
            log_and_print(
                f"Slack API returned {resp.status_code}: {resp.text}",
                "warning",
            )
            return False

        log_and_print("Slack error notification sent successfully", "info")
        return True

    except Exception as e:
        log_and_print(f"Failed to send Slack notification: {e}", "warning")
        return False


def _build_blocks(stats: Dict[str, Any]) -> list:
    """Build Slack Block Kit blocks from the sync stats dict."""
    error_count = stats.get("errors", 0)
    total_tools = stats.get("total_tools", 0)

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":warning: Tool Sync — {error_count} error{'s' if error_count != 1 else ''}",
                "emoji": True,
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                _field("Total tools", total_tools),
                _field("Errors", error_count),
                _field("Created", stats.get("created", 0)),
                _field("Updated", stats.get("updated", 0)),
                _field("Converted", stats.get("converted", 0)),
                _field("Marked unavailable", stats.get("marked_unavailable", 0)),
                _field("Marked available", stats.get("marked_available", 0)),
                _field(
                    "Updated then unavailable",
                    stats.get("update_then_mark_unavailable", 0),
                ),
            ],
        },
    ]

    error_details = stats.get("error_details", [])
    if error_details:
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error details (showing up to 10 of {len(error_details)}):*",
                },
            }
        )

        for err in error_details[:10]:
            serial = err.get("serial_number", "UNKNOWN")
            action = err.get("action", "?")
            msg = err.get("error", "No message")
            if len(msg) > 150:
                msg = msg[:147] + "..."
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"• `{serial}` ({action}): {msg}",
                    },
                }
            )

        if len(error_details) > 10:
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"_{len(error_details) - 10} more error(s) omitted — check the Databricks run output for full details._",
                        }
                    ],
                }
            )

    return blocks


def _field(label: str, value: Any) -> dict:
    return {"type": "mrkdwn", "text": f"*{label}:*\n{value}"}
