import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import subscription_manager as sm  # noqa: E402

CALLBACK_HOST = "https://callback.example.com"
CALLBACK_PASSWORD = "our-password"

ENV = {
    "NS_API_HOST": "https://sipns.example.net",
    "CALLBACK_HOST": CALLBACK_HOST,
    "CALLBACK_PASSWORD": CALLBACK_PASSWORD,
    "CLOUD_ID": "NEW_CLOUD",
    "NS_USERNAME": "user@domain",
    "NS_PASSWORD": "secret",
    "NS_CLIENT_ID": "client",
    "NS_CLIENT_SECRET": "client-secret",
}


def make_subscription(subscription_id: str, post_url: str, domain: str = "DomainA") -> sm.Subscription:
    return sm.Subscription(
        id=subscription_id,
        domain=domain,
        model=sm.SubscriptionModel.MESSAGE,
        current_active_server="sipns.example.net",
        subscription_expires="2045-01-01 00:00:00",
        post_url=post_url,
        subscription_geo_support="yes",
        user_scope="Super User",
        reseller="Reseller",
        user="*",
        preferred_server="sipns.example.net",
        status="active",
        error_count=0,
        posts_count=0,
        subscription_creation="2025-01-01T00:00:00+00:00",
    )


MATCHING = [
    make_subscription("new-message", f"{CALLBACK_HOST}/netsapiens/callbacks/NEW_CLOUD/message?password=our-password"),
    make_subscription("old-session", f"{CALLBACK_HOST}/netsapiens/callbacks/OLD_CLOUD/message-session?password=old-secret"),
    make_subscription("old-editable", f"{CALLBACK_HOST}/netsapiens/callbacks/OLD_CLOUD*/message?password=old-secret"),
]
NOT_MATCHING = [
    make_subscription("foreign-host", "https://foreign.example.com/netsapiens/callbacks/OLD_CLOUD/message?password=x"),
    make_subscription("other-scheme", "http://callback.example.com/netsapiens/callbacks/OLD_CLOUD/message?password=x"),
    make_subscription("domain-subscription", f"{CALLBACK_HOST}/netsapiens/callbacks/OLD_CLOUD/domain/DomainA/message?password=x"),
    make_subscription("other-path", f"{CALLBACK_HOST}/netsapiens/callbacks/OLD_CLOUD/subscriptions-status?password=x"),
    make_subscription("trailing-segment", f"{CALLBACK_HOST}/netsapiens/callbacks/OLD_CLOUD/message/extra?password=x"),
    make_subscription("other-prefix", f"{CALLBACK_HOST}/other/netsapiens/callbacks/OLD_CLOUD/message?password=x"),
    make_subscription("empty-post-url", ""),
]


class PurgePatternTest(unittest.TestCase):

    def test_matching_post_urls_of_any_cloud_id(self):
        for subscription in MATCHING:
            with self.subTest(subscription=subscription.id):
                self.assertTrue(sm.is_purgeable_subscription(subscription, CALLBACK_HOST))

    def test_not_matching_post_urls(self):
        for subscription in NOT_MATCHING:
            with self.subTest(subscription=subscription.id):
                self.assertFalse(sm.is_purgeable_subscription(subscription, CALLBACK_HOST))


class PurgeSubscriptionsTest(unittest.TestCase):

    def setUp(self):
        env = mock.patch.dict(os.environ, ENV, clear=True)
        env.start()
        self.addCleanup(env.stop)
        self.mocks = {}
        for name, kwargs in {
            "get_authenticated_api_client": {"return_value": sm.AuthResult(client=mock.Mock(), user_scope="Super User")},
            "fetch_subscriptions": {"return_value": MATCHING + NOT_MATCHING},
            "delete_subscription": {"return_value": True},
            "create_subscription": {},
            "update_subscription": {},
            "fetch_active_domains": {},
            "fetch_failing_subscriptions": {},
            "send_status_report": {},
            "send_script_log": {"return_value": True},
        }.items():
            patcher = mock.patch.object(sm, name, **kwargs)
            self.mocks[name] = patcher.start()
            self.addCleanup(patcher.stop)

    def deleted_ids(self):
        return [call.args[1] for call in self.mocks["delete_subscription"].call_args_list]

    def assert_no_regular_processing(self):
        for name in ("create_subscription", "update_subscription", "fetch_active_domains",
                     "fetch_failing_subscriptions", "send_status_report"):
            self.mocks[name].assert_not_called()

    def test_dry_run_lists_matching_subscriptions_without_deleting(self):
        with mock.patch("builtins.print") as printed:
            result = sm.purge_subscriptions(sm.ProcessingContext(), execute=False)

        self.assertEqual([s.id for s in result.matched_subscriptions], [s.id for s in MATCHING])
        self.mocks["delete_subscription"].assert_not_called()
        self.mocks["send_script_log"].assert_not_called()
        self.assert_no_regular_processing()
        output = " ".join(str(call.args) for call in printed.call_args_list)
        self.assertNotIn("our-password", output)
        self.assertNotIn("old-secret", output)

    def test_execute_deletes_all_matching_subscriptions_of_all_cloud_ids(self):
        context = sm.ProcessingContext()
        sm.purge_subscriptions(context, execute=True)

        self.assertEqual(self.deleted_ids(), [s.id for s in MATCHING])
        self.assertEqual(context.stage, sm.ProcessingStage.PURGE_SUBSCRIPTIONS)
        self.assertEqual(self.mocks["send_script_log"].call_args.kwargs["level"], "INFO")
        self.assert_no_regular_processing()

    def test_failed_delete_continues_and_fails_the_run(self):
        self.mocks["delete_subscription"].side_effect = lambda _client, subscription_id: subscription_id != "old-session"
        context = sm.ProcessingContext()

        with self.assertRaises(RuntimeError) as raised:
            sm.purge_subscriptions(context, execute=True)

        self.assertEqual(self.deleted_ids(), [s.id for s in MATCHING])
        self.assertIn("old-session", str(raised.exception))
        self.assertEqual(sm._infer_error_code(context.stage), sm.ErrorCode.SUBSCRIPTION_MUTATION_FAILURE)
        self.assertEqual(self.mocks["send_script_log"].call_args.kwargs["level"], "ERROR")

    def test_failed_subscription_fetch_deletes_nothing(self):
        self.mocks["fetch_subscriptions"].return_value = None

        with self.assertRaises(RuntimeError):
            sm.purge_subscriptions(sm.ProcessingContext(), execute=True)

        self.mocks["delete_subscription"].assert_not_called()


class ArgumentsTest(unittest.TestCase):

    def test_regular_run_without_arguments(self):
        args = sm.parse_arguments([])

        self.assertFalse(args.purge_subscriptions)
        self.assertEqual(sm.describe_run_mode(args), "")

    def test_purge_defaults_to_dry_run(self):
        args = sm.parse_arguments(["--purge-subscriptions"])

        self.assertFalse(args.execute)
        self.assertEqual(sm.describe_run_mode(args), " (purge mode, dry run)")

    def test_execute_requires_purge(self):
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
            sm.parse_arguments(["--execute"])

    def test_run_dispatches_by_mode(self):
        with mock.patch.object(sm, "main") as regular, mock.patch.object(sm, "purge_subscriptions") as purge:
            sm.run(sm.ProcessingContext(), sm.parse_arguments([]))
            sm.run(sm.ProcessingContext(), sm.parse_arguments(["--purge-subscriptions", "--execute"]))

        regular.assert_called_once()
        purge.assert_called_once_with(mock.ANY, execute=True)


if __name__ == "__main__":
    unittest.main()
