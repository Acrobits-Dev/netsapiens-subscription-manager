import io
import json
import os
import sys
import unittest
import urllib.error
import urllib.parse
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import subscription_manager as sm  # noqa: E402


CALLBACK_HOST = "https://callback.example.com"
FOREIGN_CALLBACK_HOST = "https://foreign.example.com"
CALLBACK_PASSWORD = "p@ss/word"
CLOUD_ID = "CLOUD"

BASE_ENV = {
    "NS_API_HOST": "https://sipns.example.net",
    "CALLBACK_HOST": CALLBACK_HOST,
    "CALLBACK_PASSWORD": CALLBACK_PASSWORD,
    "CLOUD_ID": CLOUD_ID,
    "NS_USERNAME": "user@domain",
    "NS_PASSWORD": "secret",
    "NS_CLIENT_ID": "client",
    "NS_CLIENT_SECRET": "client-secret",
}


def make_config(cloud_id: str = CLOUD_ID) -> sm.Config:
    return sm.Config(
        ns_api_host="https://sipns.example.net",
        callback_host=CALLBACK_HOST,
        callback_password=CALLBACK_PASSWORD,
        cloud_id=cloud_id,
    )


def make_callback_config(cloud_id: str = CLOUD_ID) -> sm.CallbackConfig:
    return sm.CallbackConfig(callback_host=CALLBACK_HOST, callback_password=CALLBACK_PASSWORD, cloud_id=cloud_id)


def make_selection(active, available, editable=()) -> sm.DomainSelection:
    domain_infos = [sm.DomainInfo(domain=domain, reseller=f"{domain}_reseller") for domain in available]
    return sm.compute_domain_selection(sm.ActiveDomains(list(active), list(editable)), domain_infos)


def post_url(cloud_id: str, model: sm.SubscriptionModel, host: str = CALLBACK_HOST) -> str:
    encoded_password = urllib.parse.quote(CALLBACK_PASSWORD, safe="")
    return f"{host}/netsapiens/callbacks/{cloud_id}/{model.to_path_suffix()}?password={encoded_password}"


def make_subscription(
    subscription_id: str,
    domain: str,
    model: sm.SubscriptionModel,
    cloud_id: str = CLOUD_ID,
    host: str = CALLBACK_HOST,
    current_active_server: str = "sipns.example.net",
) -> sm.Subscription:
    return sm.Subscription(
        id=subscription_id,
        domain=domain,
        model=model,
        current_active_server=current_active_server,
        subscription_expires="2045-01-01 00:00:00",
        post_url=post_url(cloud_id, model, host),
        subscription_geo_support="yes",
        user_scope="Super User",
        reseller=f"{domain}_reseller",
        user="*",
        preferred_server="sipns.example.net",
        status="active",
        error_count=0,
        posts_count=0,
        subscription_creation="2025-01-01T00:00:00+00:00",
    )


class FakeResponse:
    def __init__(self, body: str):
        self._body = body.encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://callback.example.com", code, "error", {}, io.BytesIO(b""))


class FetchActiveDomainsTest(unittest.TestCase):

    def fetch_with_body(self, body: str, cloud_id: str = CLOUD_ID):
        with mock.patch("urllib.request.urlopen", return_value=FakeResponse(body)) as urlopen:
            result = sm.fetch_active_domains(make_callback_config(cloud_id))
        return result, urlopen

    def test_request_uses_exact_encoded_cloud_id_encoded_password_timeout_and_no_bearer_token(self):
        body = json.dumps({"activeDomains": ["DomainA"], "editableDomains": []})

        result, urlopen = self.fetch_with_body(body, cloud_id="CLOUD*")

        request = urlopen.call_args.args[0]
        self.assertEqual(
            request.full_url,
            f"{CALLBACK_HOST}/netsapiens/callbacks/CLOUD%2A/active-domains?password=p%40ss%2Fword",
        )
        self.assertEqual(request.get_method(), "GET")
        self.assertIsNone(request.get_header("Authorization"))
        self.assertIsNone(request.get_header("X-ntspns-api-key"))
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 15.0)
        self.assertEqual(result.active_domains, ["DomainA"])

    def test_both_lists_are_returned_and_deduplicated_without_changing_case(self):
        body = json.dumps({
            "activeDomains": ["DomainA", "DomainB", "DomainA", "domaina"],
            "editableDomains": ["DomainB", "DomainB"],
        })

        result, _ = self.fetch_with_body(body)

        self.assertEqual(result.active_domains, ["DomainA", "DomainB", "domaina"])
        self.assertEqual(result.editable_domains, ["DomainB"])

    def test_empty_lists_are_valid_result(self):
        result, _ = self.fetch_with_body(json.dumps({"activeDomains": [], "editableDomains": []}))

        self.assertEqual(result.active_domains, [])
        self.assertEqual(result.editable_domains, [])

    def test_invalid_payloads_are_rejected(self):
        invalid_bodies = [
            "",
            "not-json",
            json.dumps([]),
            json.dumps({"activeDomains": ["DomainA"]}),
            json.dumps({"editableDomains": []}),
            json.dumps({"activeDomains": None, "editableDomains": []}),
            json.dumps({"activeDomains": ["DomainA"], "editableDomains": None}),
            json.dumps({"activeDomains": ["DomainA", 1], "editableDomains": []}),
            json.dumps({"activeDomains": ["DomainA", " "], "editableDomains": []}),
            json.dumps({"activeDomains": "DomainA", "editableDomains": []}),
        ]
        for body in invalid_bodies:
            with self.subTest(body=body):
                with self.assertRaises(RuntimeError):
                    self.fetch_with_body(body)

    def test_transport_failures_are_rejected(self):
        failures = [
            http_error(401),
            http_error(404),
            http_error(500),
            urllib.error.URLError("unreachable"),
            TimeoutError("timed out"),
        ]
        for failure in failures:
            with self.subTest(failure=failure):
                with mock.patch("urllib.request.urlopen", side_effect=failure):
                    with self.assertRaises(Exception):
                        sm.fetch_active_domains(make_callback_config())

    def test_password_is_not_printed(self):
        with mock.patch("urllib.request.urlopen", side_effect=http_error(401)), \
                mock.patch("builtins.print") as printed:
            with self.assertRaises(RuntimeError) as raised:
                sm.fetch_active_domains(make_callback_config())

        output = " ".join(str(call.args) for call in printed.call_args_list) + str(raised.exception)
        self.assertNotIn(CALLBACK_PASSWORD, output)
        self.assertNotIn(urllib.parse.quote(CALLBACK_PASSWORD, safe=""), output)


class RemovedConfigurationTest(unittest.TestCase):

    def test_removed_domain_variables_are_ignored_even_in_previously_invalid_combinations(self):
        env = dict(BASE_ENV)
        env.update({
            "ALLOWED_DOMAINS": "DomainA",
            "DISALLOWED_DOMAINS": "DomainB",
            "NS_RESELLERS": "Reseller",
            "EDITABLE_VERSION_DOMAIN": "DomainC",
        })
        with mock.patch.dict(os.environ, env, clear=True):
            config, _ = sm.load_all_config_from_env()

        self.assertEqual(config, make_config())
        self.assertEqual(
            set(vars(config).keys()),
            {"ns_api_host", "callback_host", "callback_password", "cloud_id"},
        )

    def test_editable_version_domain_variable_does_not_route_callbacks(self):
        selection = make_selection(active=["DomainC"], available=["DomainC"])
        with mock.patch.dict(os.environ, {"EDITABLE_VERSION_DOMAIN": "DomainC"}):
            url = sm._build_post_url(sm.SubscriptionModel.MESSAGE, "DomainC", make_config(), selection)

        self.assertIn(f"/netsapiens/callbacks/{CLOUD_ID}/message?", url)


class DomainSelectionTest(unittest.TestCase):

    def test_selection_is_intersection_and_unknown_active_domains_are_missing(self):
        selection = make_selection(active=["DomainB", "Unknown", "DomainA"], available=["DomainA", "DomainB", "DomainC"])

        self.assertEqual(selection.selected_domain_names, ["DomainA", "DomainB"])
        self.assertEqual(selection.missing_active_domains, ["Unknown"])

    def test_domain_names_are_compared_exactly(self):
        selection = make_selection(active=["domaina"], available=["DomainA"])

        self.assertEqual(selection.selected_domain_names, [])
        self.assertEqual(selection.missing_active_domains, ["domaina"])

    def test_empty_active_domains_select_nothing(self):
        selection = make_selection(active=[], available=["DomainA"], editable=["DomainA"])

        self.assertEqual(selection.selected_domain_names, [])


class ReviewSubscriptionsTest(unittest.TestCase):

    def review(self, subscriptions, active, available, editable=()):
        domain_infos = [sm.DomainInfo(domain=domain, reseller=f"{domain}_reseller") for domain in available]
        selection = make_selection(active, available, editable)
        return sm.review_subscriptions(subscriptions, domain_infos, make_config(), selection)

    def test_new_active_domain_gets_both_models_and_existing_one_is_not_duplicated(self):
        subscriptions = [
            make_subscription("1", "DomainA", sm.SubscriptionModel.MESSAGE),
            make_subscription("2", "DomainA", sm.SubscriptionModel.MESSAGE_SESSION),
        ]

        review = self.review(subscriptions, active=["DomainA", "DomainB"], available=["DomainA", "DomainB"])

        self.assertEqual(review.domains_without_message_subscription, ["DomainB"])
        self.assertEqual(review.domains_without_messagesession_subscription, ["DomainB"])
        self.assertEqual(review.subscriptions_in_inactive_domains, [])

    def test_inactive_domain_subscriptions_are_removed_and_not_recreated(self):
        subscriptions = [make_subscription("1", "DomainB", sm.SubscriptionModel.MESSAGE)]

        review = self.review(subscriptions, active=["DomainA"], available=["DomainA", "DomainB"])

        self.assertEqual([s.id for s in review.subscriptions_in_inactive_domains], ["1"])
        self.assertNotIn("DomainB", review.domains_without_message_subscription)
        self.assertNotIn("DomainB", review.domains_without_messagesession_subscription)

    def test_empty_active_domains_remove_only_managed_subscriptions(self):
        own = make_subscription("1", "DomainA", sm.SubscriptionModel.MESSAGE)
        foreign = make_subscription("2", "DomainA", sm.SubscriptionModel.MESSAGE, host=FOREIGN_CALLBACK_HOST)

        review = self.review([own, foreign], active=[], available=["DomainA"])

        self.assertEqual([s.id for s in review.subscriptions_in_inactive_domains], ["1"])
        self.assertEqual(review.domains_without_message_subscription, [])
        self.assertEqual(review.domains_without_messagesession_subscription, [])

    def test_active_domain_unknown_to_netsapiens_is_reported_and_not_created(self):
        review = self.review([], active=["DomainA", "Unknown"], available=["DomainA"])

        self.assertEqual(review.missing_active_domains, ["Unknown"])
        self.assertNotIn("Unknown", review.domains_without_message_subscription)

    def test_domain_only_in_editable_domains_is_not_created(self):
        review = self.review([], active=["DomainA"], available=["DomainA", "DomainE"], editable=["DomainE"])

        self.assertEqual(review.domains_without_message_subscription, ["DomainA"])

    def test_editable_change_marks_existing_subscription_post_url_as_outdated(self):
        subscriptions = [make_subscription("1", "DomainA", sm.SubscriptionModel.MESSAGE, cloud_id=CLOUD_ID)]

        review = self.review(subscriptions, active=["DomainA"], available=["DomainA"], editable=["DomainA"])

        self.assertEqual([s.id for s in review.subscriptions_with_outdated_post_url], ["1"])

    def test_foreign_subscription_is_untouched(self):
        foreign = make_subscription("2", "DomainB", sm.SubscriptionModel.MESSAGE, host=FOREIGN_CALLBACK_HOST)

        review = self.review([foreign], active=["DomainA"], available=["DomainA", "DomainB"])

        self.assertEqual(review.subscriptions_in_inactive_domains, [])
        self.assertEqual(review.subscriptions_in_unknown_domains, [])
        self.assertEqual(review.subscriptions_with_outdated_post_url, [])


class EditableRoutingTest(unittest.TestCase):

    def build(self, domain, editable, cloud_id=CLOUD_ID):
        selection = make_selection(active=[domain], available=[domain], editable=editable)
        return sm._build_post_url(sm.SubscriptionModel.MESSAGE_SESSION, domain, make_config(cloud_id), selection)

    def test_multiple_editable_domains_route_to_editable_cloud_id(self):
        self.assertIn("/callbacks/CLOUD*/message-session?", self.build("DomainA", ["DomainA", "DomainB"]))
        self.assertIn("/callbacks/CLOUD*/message-session?", self.build("DomainB", ["DomainA", "DomainB"]))

    def test_other_domains_route_to_live_cloud_id(self):
        self.assertIn("/callbacks/CLOUD/message-session?", self.build("DomainC", ["DomainA"]))
        self.assertIn("/callbacks/CLOUD/message-session?", self.build("DomainC", []))

    def test_editable_membership_is_case_insensitive(self):
        self.assertIn("/callbacks/CLOUD*/message-session?", self.build("DomainA", ["domaina"]))

    def test_suffix_is_not_added_twice(self):
        self.assertIn("/callbacks/CLOUD*/message-session?", self.build("DomainA", ["DomainA"], cloud_id="CLOUD*"))

    def test_create_body_keeps_original_domain_name(self):
        selection = make_selection(active=["DomainA"], available=["DomainA"], editable=["domaina"])
        body = sm._build_subscription_body(
            sm.SubscriptionModel.MESSAGE, "DomainA", "Reseller", "Super User", "2045-01-01 00:00:00",
            make_config(), selection,
        )

        self.assertEqual(body["domain"], "DomainA")
        self.assertIn("/callbacks/CLOUD*/message?", body["post-url"])


class RecreateFailingSubscriptionsTest(unittest.TestCase):

    def setUp(self):
        patches = [
            mock.patch.object(sm.time, "sleep"),
            mock.patch.object(sm, "send_script_log", return_value=True),
        ]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.delete = self.start_patch("delete_subscription", return_value=True)
        self.create = self.start_patch("create_subscription", return_value="new-id")
        self.mark = self.start_patch("mark_failing_subscription_deleted", return_value=True)

    def start_patch(self, name, **kwargs):
        patcher = mock.patch.object(sm, name, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def recreate(self, failing, subscriptions, active, available):
        selection = make_selection(active, available)
        with mock.patch.object(sm, "fetch_failing_subscriptions", return_value=failing):
            sm.recreate_failing_subscriptions(
                client=mock.Mock(),
                config=make_config(),
                selection=selection,
                domain_to_reseller={domain: f"{domain}_reseller" for domain in available},
                current_subscriptions=subscriptions,
                user_scope="Super User",
                subscription_expires="2045-01-01 00:00:00",
            )

    def test_active_domain_is_recreated(self):
        subscription = make_subscription("old", "DomainA", sm.SubscriptionModel.MESSAGE)

        self.recreate([sm.FailingSubscription(CLOUD_ID, "DomainA", "old")], [subscription], ["DomainA"], ["DomainA"])

        self.delete.assert_called_once_with(mock.ANY, "old")
        self.create.assert_called_once()
        self.assertIsInstance(self.create.call_args.kwargs["selection"], sm.DomainSelection)
        self.mark.assert_called_once_with(mock.ANY, "old")

    def test_inactive_domain_is_deleted_without_replacement_and_record_marked(self):
        subscription = make_subscription("old", "DomainB", sm.SubscriptionModel.MESSAGE)

        self.recreate([sm.FailingSubscription(CLOUD_ID, "DomainB", "old")], [subscription], ["DomainA"], ["DomainA", "DomainB"])

        self.delete.assert_called_once_with(mock.ANY, "old")
        self.create.assert_not_called()
        self.mark.assert_called_once_with(mock.ANY, "old")

    def test_failed_delete_of_inactive_domain_does_not_mark_record(self):
        self.delete.return_value = False
        subscription = make_subscription("old", "DomainB", sm.SubscriptionModel.MESSAGE)

        self.recreate([sm.FailingSubscription(CLOUD_ID, "DomainB", "old")], [subscription], ["DomainA"], ["DomainA", "DomainB"])

        self.create.assert_not_called()
        self.mark.assert_not_called()

    def test_unknown_or_foreign_subscription_id_is_not_deleted_nor_marked(self):
        self.recreate([sm.FailingSubscription(CLOUD_ID, "DomainB", "foreign")], [], ["DomainA"], ["DomainA", "DomainB"])

        self.delete.assert_not_called()
        self.create.assert_not_called()
        self.mark.assert_not_called()

    def test_decision_uses_domain_of_actual_subscription(self):
        subscription = make_subscription("old", "DomainB", sm.SubscriptionModel.MESSAGE)

        self.recreate([sm.FailingSubscription(CLOUD_ID, "DomainA", "old")], [subscription], ["DomainA"], ["DomainA", "DomainB"])

        self.create.assert_not_called()
        self.mark.assert_called_once_with(mock.ANY, "old")


class StatusReportTest(unittest.TestCase):

    def test_report_matches_selection_and_keeps_configuration_contract(self):
        domain_infos = [sm.DomainInfo("DomainA", "ResellerA"), sm.DomainInfo("DomainB", "ResellerB")]
        selection = make_selection(active=["DomainA", "Unknown"], available=["DomainA", "DomainB"], editable=["DomainA", "DomainX"])
        subscriptions = [make_subscription("1", "DomainA", sm.SubscriptionModel.MESSAGE, cloud_id="CLOUD*")]

        report = sm.build_status_report(subscriptions, domain_infos, make_config(), selection, "2026-01-01T00:00:00Z").to_dict()

        self.assertEqual(report["configuration"]["allowed_domains"], ["DomainA", "Unknown"])
        self.assertEqual(report["configuration"]["disallowed_domains"], [])
        self.assertEqual(report["configuration"]["resellers"], [])
        self.assertEqual(report["configuration"]["editable_version_domain"], "")
        self.assertEqual(report["summary"]["effective_domains_count"], 1)
        domains = {domain["domain"]: domain for domain in report["domains"]}
        self.assertEqual(domains["DomainA"]["coverage_status"], "PARTIALLY_COVERED")
        self.assertEqual(domains["DomainB"]["coverage_status"], "EXCLUDED")
        self.assertEqual(domains["DomainB"]["exclusion_reason"], "NOT_IN_ALLOWED_LIST")


class MainFlowTest(unittest.TestCase):

    def setUp(self):
        self.env = mock.patch.dict(os.environ, BASE_ENV, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.patches = {}
        for name, kwargs in {
            "get_authenticated_api_client": {"return_value": sm.AuthResult(client=mock.Mock(), user_scope="Super User")},
            "get_domain_list": {"return_value": [sm.DomainInfo("DomainA", "ResellerA"), sm.DomainInfo("DomainB", "ResellerB")]},
            "fetch_subscriptions": {"return_value": [make_subscription("1", "DomainB", sm.SubscriptionModel.MESSAGE)]},
            "fetch_failing_subscriptions": {"return_value": []},
            "delete_subscription": {"return_value": True},
            "update_subscription": {"return_value": True},
            "create_subscription": {"return_value": "new-id"},
            "send_status_report": {"return_value": True},
            "send_script_log": {"return_value": True},
            "mark_failing_subscription_deleted": {"return_value": True},
        }.items():
            patcher = mock.patch.object(sm, name, **kwargs)
            self.patches[name] = patcher.start()
            self.addCleanup(patcher.stop)

    def test_failed_active_domains_fetch_stops_before_any_netsapiens_call(self):
        context = sm.ProcessingContext()
        with mock.patch.object(sm, "fetch_active_domains", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                sm.main(context)

        self.assertEqual(context.stage, sm.ProcessingStage.FETCH_ACTIVE_DOMAINS)
        self.assertEqual(sm._infer_error_code(context.stage), sm.ErrorCode.ACTIVE_DOMAINS_RETRIEVAL_FAILURE)
        for name in ("get_authenticated_api_client", "delete_subscription", "update_subscription", "create_subscription"):
            self.patches[name].assert_not_called()

    def test_active_domains_are_fetched_once_and_drive_changes_and_report(self):
        active_domains = sm.ActiveDomains(active_domains=["DomainA"], editable_domains=["DomainA"])
        with mock.patch.object(sm, "fetch_active_domains", return_value=active_domains) as fetch:
            sm.main(sm.ProcessingContext())

        fetch.assert_called_once()
        self.patches["delete_subscription"].assert_called_once_with(mock.ANY, "1")
        created = {(call.args[1], call.args[2]) for call in self.patches["create_subscription"].call_args_list}
        self.assertEqual(created, {(sm.SubscriptionModel.MESSAGE, "DomainA"), (sm.SubscriptionModel.MESSAGE_SESSION, "DomainA")})
        report = self.patches["send_status_report"].call_args.kwargs["report"].to_dict()
        self.assertEqual(report["configuration"]["allowed_domains"], ["DomainA"])
        self.assertEqual(report["summary"]["effective_domains_count"], 1)

    def test_empty_active_domains_create_nothing(self):
        active_domains = sm.ActiveDomains(active_domains=[], editable_domains=["DomainA"])
        with mock.patch.object(sm, "fetch_active_domains", return_value=active_domains):
            sm.main(sm.ProcessingContext())

        self.patches["create_subscription"].assert_not_called()
        self.patches["delete_subscription"].assert_called_once_with(mock.ANY, "1")


if __name__ == "__main__":
    unittest.main()
