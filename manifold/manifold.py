def main():
    import sys
    import gatling
#    import cProfile

    sys.argv.insert(1, 'manifold')

#    pr = cProfile.Profile()
#    pr.enable()
    gatling.main()
#    pr.create_stats()
 #   pr.dump_stats(f"/tmp/manifold.prof")
 #   print("Dumped profile in", f"/tmp/manifold.prof", file=sys.stderr)
 #   echo -n 'sort\nstats'|python -m pstats /tmp/manifold.prof


##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
