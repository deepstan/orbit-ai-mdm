"""Pins a test class to the LEGACY matching engine (MDM_ENGINE=legacy).

The application's default is the kernel (app/kernel_router.py). Some tests exist to prove that the older engines, which are kept for rollback and for comparison, still agree with each
other: the set-based bulk and onboarding engines against the legacy one-at-a-time engine and the pairwise reference matcher. Those tests are about the LEGACY engines, so they say
so. The setting lasts for the class only (it is restored afterwards, even if a test fails), so it cannot leak into the tests of the kernel.
"""
import os
import unittest.mock as mock


def legacy_engine(cls):
    orig_up, orig_down = cls.setUpClass, cls.tearDownClass

    def set_up_class(klass):
        klass._engine_pin = mock.patch.dict(os.environ, {"MDM_ENGINE": "legacy"})
        klass._engine_pin.start()
        try:
            orig_up()
        except BaseException:
            klass._engine_pin.stop()
            raise

    def tear_down_class(klass):
        try:
            orig_down()
        finally:
            klass._engine_pin.stop()

    cls.setUpClass, cls.tearDownClass = classmethod(set_up_class), classmethod(tear_down_class)
    return cls
