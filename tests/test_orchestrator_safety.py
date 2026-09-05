"""Exercise the actual breaker and POSIX child lifecycle, not substituted methods."""
import argparse
from decimal import Decimal
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestrator import BotProcess, PnlTracker, bot_environment, utc_now


class SafetyBoundaryTests(unittest.TestCase):
    def test_equity_and_execution_loss_breakers_fire_at_the_limit(self):
        with tempfile.TemporaryDirectory() as td:
            args=argparse.Namespace(state_dir=Path(td),market="HYPE",pnl_since="now",max_loss_usdc=Decimal(15),baseline_max_gap_hours=48)
            tracker=PnlTracker(args,utc_now(),lambda *a,**k:None)
            def equity(value):
                tracker.record({"bot":"XEMM","accounts":{"total_equity_usd":str(value)}},"XEMM")
            equity(100)
            equity("85.01")
            self.assertIsNone(tracker.breaker_reason({"risk_net_pnl_usdc":"0"}))
            equity(85)
            self.assertIn("equity_drawdown",tracker.breaker_reason({"risk_net_pnl_usdc":"0"}))
            equity(100)
            self.assertIsNone(tracker.breaker_reason({"risk_net_pnl_usdc":"-14.99"}))
            self.assertIsNotNone(tracker.breaker_reason({"net_pnl_usdc":None,"risk_net_pnl_usdc":"-15"}))

    def test_real_children_stop_gracefully_and_after_signal_escalation(self):
        for ignore_signals in (False,True):
            with self.subTest(ignore_signals=ignore_signals),tempfile.TemporaryDirectory() as td:
                root=Path(td)
                program="import os,signal,time\n"
                if ignore_signals:
                    program+="signal.signal(signal.SIGINT,signal.SIG_IGN)\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\n"
                program+="print(os.environ['ASTER_NONCE_DIR'],flush=True)\nprint('ready',flush=True)\nwhile True: time.sleep(1)\n"
                env=bot_environment(argparse.Namespace(aster_nonce_dir=root/"nonces"))
                child=BotProcess("test-child",root,[sys.executable,"-u","-c",program],root/"child.log",env)
                try:
                    child.start()
                    deadline=time.monotonic()+5
                    while time.monotonic()<deadline and "ready" not in child.log_path.read_text():
                        time.sleep(0.01)
                    self.assertIn("ready",child.log_path.read_text())
                    self.assertIn(env["ASTER_NONCE_DIR"],child.log_path.read_text())
                    result=child.stop(1)
                    self.assertFalse(child.is_running())
                    self.assertEqual(result["signal"],"SIGKILL" if ignore_signals else "SIGINT")
                    self.assertIsNotNone(result["exit_code"])
                finally:
                    # Also cleans up when a mutation deliberately disables BotProcess.stop.
                    if child.proc is not None and child.proc.poll() is None:
                        os.killpg(child.proc.pid,signal.SIGKILL)
                        child.proc.wait(timeout=5)
                    child._close_log()

if __name__=="__main__":
    unittest.main()
