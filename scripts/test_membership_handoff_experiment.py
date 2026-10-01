import copy
import unittest

from membership_handoff_experiment import CompositionMixin, validate_ticket, validate_value


def ticket():
    operation = dict(kind="set_voters", voters=[1, 4, 5])
    return dict(request_id="replacement", target=1, operation=operation,
        origin=dict(group=0, index=10), previous_generation=None,
        completion=dict(record=dict(operation=operation, first_index=20, first_term=2,
            joint=True, final_index=21, final_term=2),
            observed_applied=dict(group=1, index=21), released=dict(group=0, index=30)))


class OracleTests(unittest.TestCase):
    def test_completed_ticket_requires_exact_applied_record(self):
        value = ticket()
        self.assertEqual(validate_ticket(value, "replacement", 1, value["operation"], True), value)
        for change in (lambda x: x["completion"]["observed_applied"].update(index=20),
                       lambda x: x["completion"]["record"].update(final_index=20),
                       lambda x: x["completion"]["record"].update(joint=False),
                       lambda x: x["completion"].update(released=dict(group=0, index=10)),
                       lambda x: x.update(previous_generation=dict(group=0, index=10))):
            altered = copy.deepcopy(value)
            change(altered)
            with self.subTest(altered=altered), self.assertRaises(AssertionError):
                validate_ticket(altered, "replacement", 1, value["operation"], True)

    def test_booleans_cannot_impersonate_group_or_voter_ids(self):
        value = ticket()
        for change in (lambda x: x.update(target=True),
                       lambda x: x["completion"]["observed_applied"].update(group=True),
                       lambda x: x["completion"]["record"].update(first_term=True),
                       lambda x: x["operation"].update(voters=[True, 4, 5])):
            altered = copy.deepcopy(value)
            change(altered)
            with self.subTest(altered=altered), self.assertRaises(AssertionError):
                validate_ticket(altered, "replacement", 1, value["operation"], True)

    def test_missing_or_wrong_operation_cannot_resolve_unknown(self):
        value = ticket()
        for change in (lambda x: x.pop("completion"),
                       lambda x: x.update(completion=None),
                       lambda x: x["completion"]["record"].update(operation=dict(kind="remove", id=1))):
            altered = copy.deepcopy(value)
            change(altered)
            with self.subTest(altered=altered), self.assertRaises(AssertionError):
                validate_ticket(altered, "replacement", 1, value["operation"], True)

    def test_controller_membership_cannot_precede_its_ticket_or_follow_release(self):
        value = ticket()
        value["target"] = 0
        value["completion"]["observed_applied"]["group"] = 0
        validate_ticket(value, "replacement", 0, value["operation"], True)
        for change in (lambda x: x["completion"]["record"].update(first_index=9),
                       lambda x: x["completion"]["observed_applied"].update(index=30)):
            altered = copy.deepcopy(value)
            change(altered)
            with self.subTest(altered=altered), self.assertRaises(AssertionError):
                validate_ticket(altered, "replacement", 0, value["operation"], True)

    def test_absent_value_field_is_not_a_verified_deletion(self):
        value = dict(kind="value", group=2, shard=0, epoch=2, key="deleted", value=None)
        validate_value(value, 2, 2, "deleted", None)
        for change in (lambda x: x.pop("value"), lambda x: x.update(epoch=3),
                       lambda x: x.update(group=True), lambda x: x.update(key="different")):
            altered = dict(value)
            change(altered)
            with self.subTest(altered=altered), self.assertRaises(AssertionError):
                validate_value(altered, 2, 2, "deleted", None)

    def test_http_application_error_is_not_an_acknowledgement(self):
        fixture = object.__new__(CompositionMixin)
        with self.assertRaises(AssertionError):
            fixture.applied(dict(outcome="applied", group=0, index=12,
                                 reply=dict(kind="rejected", reply="busy")), 0)
        with self.assertRaises(AssertionError):
            fixture.applied(dict(outcome="unknown"), 0)

    def test_unavailability_is_not_evidence_of_an_interlock(self):
        fixture = object.__new__(CompositionMixin)
        fixture.logical_rejections = 0
        fixture.refuse(dict(outcome="rejected", error="busy"))
        self.assertEqual(fixture.logical_rejections, 1)
        with self.assertRaises(AssertionError):
            fixture.refuse(dict(outcome="unavailable", reason="target timeout"))


if __name__ == "__main__":
    unittest.main()
