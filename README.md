# o4 - Background

At salesforce we use Perforce at a very large scale. A scale that
exposes some shortcomings in p4 itself. `o4` was created to improve
reliability of a sync and increase scalability in our very large scale
CI.

What that boils down to is the rather horrendous reality that a `p4
sync` makes *most* of the changes to your local files...

## Why o4?

`o4` allows you to continue using Perforce and all the associated
tools and IDE plugins, without the uncertainty around a sync. Every
sync is guaranteed perfect, every single time. In the rare occurence
that a sync could not be met to 100%, `o4` will fail loudly. Crash and
burn. No more silent errors!

In addition to that `o4` allows some dramatic improvements to CI, more
on that in the [server/README.md](server documentation).

# Restrictions

In general, `o4` is an encapsulation of p4's syncing. This allows the use of
`o4` without otherwise modifying toolchain or workflow. However *some*
assumptions was made to allow `o4` to run efficiently.

1. Directories only; o4 is not suited for single files. State is
stored in `.o4` folder in the target directory.

2. Flat clientspecs; remapping source trees into entirely different
target trees is not supported. Some mapping works, but it is generally
discouraged.

3. Newlines in filenames are not supported.

4. Python3.6 or higher. `o4` source code makes use of f-strings.

5. o4 is available only on Linux or macOS.

## What about my...

All your tools continue to work like they normally would. That means
you can use `o4` and continue using your IDE plugins, p4v, and
such. All `o4` is, is an encapsulation of p4 that verifies everything.

# Installation

1. Install the p4 command line on the path
(https://www.perforce.com/downloads/helix-command-line-client-p4)

2. Log in: `p4 login`

3. Create a clientspec: `p4 client`

4. Build the zipapps: `make`

5. Install the zipapps: `[ $(uname) = Darwin ] && make install || sudo make install`

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


## Smart clone (seeded sync)

Smart clone uses locally available files (either copy of files or a
similar branch synced in a different directory) as a seed to quickly
sync out a large number of files.

The idea being that you have two branches, say trunk and production,
that are very similar. To spare the stress on the server, network, and
VPN (for instance), `o4` uses checksumming to determine which locally
available files are identical to the one on the server.

Alternatively, start with a somewhat dated version of the fileset in a
tarball and use that as a starting point.

Example:

```sh
cd ~/Code
o4 sync trunk  # Refresh the trunk
o4 sync prod -s trunk
```

Tarball example:

```sh
cd ~/Code
tar xfz trunk-jan-2017.tgz
o4 sync trunk -s trunk-jan-2017
```

You could also use clean with the tarball:

```sh
cd ~/Code
tar xfz trunk-jan-2017.tgz
mv trunk-jan-2017 trunk
o4 clean trunk
```

# Copyright

Helix core, P4 and Perforce are registered trademarks owned by
Perforce Software, Inc.

## o4 license and copyright

Copyright (c) 2018, salesforce.com, inc. All rights reserved.

SPDX-License-Identifier: BSD-3-Clause

For full license text, see the license.txt file in the repo root or https://opensource.org/licenses/BSD-3-Clause
