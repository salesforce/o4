o4 - p4 reliably

# Background

At salesforce we use Perforce at a very large scale. A scale that
exposes some short-comings in p4 itself. `o4` was created to improve
reliability of a sync and increase scalability in our very large scale
CI.

# Restrictions

In general, `o4` is an encapsulation of p4. This allows the use of
`o4` without otherwise modifying toolchain or workflow. However *some*
assumptions was made to allow `o4` to run efficiently.

1. Directories only; o4 is not suited for single files. State is
stored in `.o4` folder in the target directory.

2. Flat clientspecs; remapping source trees into entirely different
target trees is not supported. Some mapping works, but it is generally
discouraged.

3. Python3 only. `o4` source code makes use of f-strings.

# What about my...

All your tools continue to work like they normally would. That means
you can use `o4` and continue using your IDE plugins, p4v, and such. `o4` is a mere encapsulation

# Installation

1. Install the p4 command line on the path
(https://www.perforce.com/downloads/helix-command-line-client-p4)

2. Log in: `p4 login`

3. Create a clientspec: `p4 client`

4. TBD... Probably zipapps

# Basic usage

To sync a directory (`<dir>`): `o4 sync <dir>` or from the directory:

```sh
cd <dir>
o4 sync .
```

To sync to a specific changelist:

```sh
cd <dir>
o4 sync .@<changelist>
```

To clean a directory (`<dir>`):

```sh
cd <dir>
o4 clean .
```

# Advanced capability


## Smart clone

Smart clone uses locally available files (either copy of files or a
similar branch synced in a different directory) to quickly sync out a
large number of files.

The idea being that you have two branches, say trunk and production,
that are very similar. To spare the stress on the server, network, and
VPN (for instance), `o4` uses checksumming to determine which locally
available files are identical to the one on the server.

Example:

```sh
cd ~/Code
o4 sync trunk  # Refresh the trunk
o4 sync prod -s trunk
```

## CI optimizations

Continuous integration (CI) often has a very adverse usage
pattern. Perforce is designed for clientspecs as the key to the
have-list and files are synced incrementally. Extending over and over
from the previous position.

CI is usually the opposite. Create a clientspec, flush, sync out the
few files that are different from the snapshot or what was baked in
the docker image. Then *delete* the clientspec.

`o4` can do most of that without the flush and without populating the
have-list.




The cli maintains a cache of fstat files in










# Copyright

Helix core, P4 and Perforce are registered trademarks owned by
Perforce Software, Inc.
