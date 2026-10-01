import copy
import unittest
from handoff_experiment import Case, validate_receipt, validate_closed_rejection, validate_checked

class ReceiptOracleTests(unittest.TestCase):
    def setUp(self):
        self.action=dict(action="mutate",shard=0,epoch=1,session=dict(group=1,index=3),sequence=2)
        self.receipt=dict(origin=dict(group=1,index=7),shard=0,epoch=1,session=dict(group=1,index=3),sequence=2)
        self.response=dict(group=1,index=7)

    def test_fresh_receipt_requires_submitted_group_session_sequence_epoch(self):
        validate_receipt(self.response,self.receipt,1,self.action)
        for field,value in (("session",dict(group=2,index=3)),("sequence",3),("epoch",2),("shard",1),("origin",dict(group=2,index=7))):
            bad=copy.deepcopy(self.receipt);bad[field]=value
            with self.subTest(field=field), self.assertRaises(AssertionError):
                validate_receipt(self.response,bad,1,self.action)

    def test_cross_group_retry_keeps_exact_origin_without_comparing_unrelated_log_indices(self):
        action=dict(self.action,epoch=2)
        validate_receipt(dict(group=2,index=5),self.receipt,2,action,expected=self.receipt)
        wrong=copy.deepcopy(self.receipt);wrong["origin"]=dict(group=2,index=5)
        with self.assertRaises(AssertionError):
            validate_receipt(dict(group=2,index=5),wrong,2,action,expected=self.receipt)

    def test_boolean_zero_and_future_origin_indices_cannot_be_acknowledgements(self):
        for index in (True,0,-1,2**64):
            with self.subTest(index=index), self.assertRaises(AssertionError):
                validate_receipt(dict(group=1,index=index),self.receipt,1,self.action)
        with self.assertRaises(AssertionError):
            validate_receipt(dict(group=1,index=6),self.receipt,1,self.action,expected=self.receipt)

    def test_closed_retry_requires_applied_matching_group_and_exact_error(self):
        good=dict(outcome="applied",group=2,index=17,reply=dict(kind="rejected",reply="session_closed"))
        validate_closed_rejection(good,2)
        for field,value in (("group",1),("group",True),("index",True),("index",0),
                            ("outcome","unknown"),("reply",dict(kind="rejected",reply="stale_route"))):
            with self.subTest(field=field,value=value),self.assertRaises(AssertionError):
                validate_closed_rejection(dict(good,**{field:value}),2)

    def test_boolean_receipt_identities_and_sequences_are_rejected(self):
        action=dict(self.action,sequence=1)
        receipt=dict(self.receipt,sequence=1)
        for field in ("origin","session"):
            bad=copy.deepcopy(receipt);bad[field]["group"]=True
            with self.subTest(field=field),self.assertRaises(AssertionError):
                validate_receipt(self.response,bad,1,action)
        with self.assertRaises(AssertionError):
            validate_receipt(dict(self.response,group=True),receipt,1,action)
        with self.assertRaises(AssertionError):
            validate_receipt(self.response,dict(receipt,sequence=True),1,action)

    def test_checked_fence_rejects_boolean_and_nonintegral_numeric_fields(self):
        good=dict(outcome="checked",group=1,node=1,term=1,index=1,applied_index=1,context=1)
        validate_checked(good,1)
        for field in ("group","node","term","index","applied_index","context"):
            for bad in (True,1.0):
                with self.subTest(field=field,bad=bad),self.assertRaises(AssertionError):
                    validate_checked(dict(good,**{field:bad}),1)

class CleanupTests(unittest.TestCase):
    class Child:
        def __init__(self, pid, raced=False, kill_error=None):
            self.pid=pid;self.polls=0;self.waited=False;self.killed=False;self.returncode=0
            self.raced=raced;self.kill_error=kill_error
        def poll(self):
            self.polls+=1
            return 0 if self.raced and self.polls>1 else None
        def kill(self):
            self.killed=True
            if self.kill_error:
                raise self.kill_error
        def wait(self,timeout):
            self.waited=True
            return 0
    class Stream:
        def __init__(self,error=None):self.closed=False;self.error=error
        def close(self):
            self.closed=True
            if self.error:raise self.error
    def case(self,children):
        case=Case.__new__(Case);case.processes=dict(enumerate(children,1))
        case.handles=[];case.reservations=[];case.event=lambda *a,**k:None
        return case
    def test_exit_race_still_reaps_first_child_and_visits_second(self):
        first=self.Child(1,raced=True);second=self.Child(2)
        case=self.case([first,second])
        self.assertEqual(case.cleanup(),[])
        self.assertTrue(first.waited and second.waited and second.killed)
    def test_cleanup_continues_after_termination_event_and_close_errors(self):
        first=self.Child(1,kill_error=RuntimeError("fake termination error"));second=self.Child(2)
        case=self.case([first,second])
        case.event=lambda *a,**k:(_ for _ in ()).throw(OSError("fake event sync error"))
        broken=self.Stream(OSError("fake close error"));last=self.Stream();journal=self.Stream()
        case.handles=[broken,last];case.journal=journal
        failures=case.cleanup()
        self.assertTrue(failures)
        self.assertTrue(first.waited and second.waited and last.closed and journal.closed)

if __name__=="__main__":
    unittest.main()
