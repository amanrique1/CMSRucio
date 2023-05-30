# CMSPolicyPackage
#
# Eric Vaandering <ewv@fnal.gov>, 2022

from CMSRucioPolicy.algorithms import lfn2pfn

SUPPORTED_VERSION = ["1.30", "1.31"]


def get_algorithms():
    """
    Get the algorithms for the policy package
    """
    return {
        'lfn2pfn': {
            'cmstfc': lfn2pfn.cmstfc,
        }
    }
