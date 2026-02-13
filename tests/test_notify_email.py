"""Tests for ceradon_sam_bot.notify_email — send_email with mocked SMTP."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from ceradon_sam_bot.notify_email import send_email


class TestSendEmail:
    @patch("ceradon_sam_bot.notify_email.smtplib.SMTP")
    def test_sends_email(self, mock_smtp_cls):
        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        send_email(
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_user="user",
            smtp_pass="pass",
            to_address="to@example.com",
            from_address="from@example.com",
            subject="Test Subject",
            body_text="Hello",
        )

        mock_smtp_cls.assert_called_once_with("smtp.example.com", 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user", "pass")
        mock_server.send_message.assert_called_once()
        msg = mock_server.send_message.call_args[0][0]
        assert msg["Subject"] == "Test Subject"
        assert msg["To"] == "to@example.com"
