# o4 fstat server

The `o4` client by default retrieves the fstat listing from the
perforce server. Since the fstat listings for a given changelist are
immutable, other caching and distribution methods are possible.

One such offering is the `o4` fstat server. It's a bridge between http
and `o4 fstat`. The client can use the server, perforce, local cache
or any combination of the three.

To enable the server add the following to `~/.o4/config`:

```
o4.server.url = http://<host>:<port>
o4.username = <user>
o4.password = <pass>
o4.http_proxy = <url>
```

The keys for proxy, username and password are only required if
authentication or http_proxy is enabled between the `o4` client and
the `o4` fstat server.

# CI optimizations

Continuous integration (CI) often has a very adverse usage pattern.
Perforce is designed for clientspecs as the key to the have-list and
files are synced incrementally. Extending over and over from the
previous position.

CI is usually the opposite. Create a clientspec, flush, sync out the
few files that are different from the snapshot or what was baked in
the image. Then *delete* the clientspec.

## Skip the have list

`o4` can effectively do a perfect sync without the flush and without
populating the have-list. Notice when you run `o4 sync .` that it
outputs exactly how it executes that operation. Assuming that no files
are open for edit, the operation looks something like this:

```sh
o4 fstat ...@2000 --changed 1000 |
  manifold -m 10485760 o4 drop --checksum |
  o4 keep --case |
  o4 progress |
  gatling o4  pyforce sync |
  manifold -m 10485760 o4 drop --checksum |
  gatling o4  pyforce sync -f |
  manifold -m 10485760 o4 drop --checksum |
  o4 fail
```

Roughly speaking the command goes through all the updates from
changelist 1000 to 2000, discards files with valid checksums, syncs
the rest, verifies, force sync failed, verify again, and fail if there
is still something left.

That's followed by a command to keep the havelist up to date:

```sh
o4 fstat ...@2000 --changed 1000 |
  o4 keep --case |
  o4 drop --havelist |
  o4 progress |
  gatling -v -n 4 o4 pyforce sync -k |
  o4 drop --havelist |
  o4 fail
```

This have-list command is entirely optional. It keeps the state in the
have-list up to date, but if you don't need the have-list for
anything, it can be skipped.

Assume that we have a nightly tarball/snapshot of trunk:

```sh
o4 sync trunk
tar cfz trunk-nightly.tgz
```

To quickly sync to a given changelist `<cl>` in a CI such as Jenkins,
we would just:

```sh
# Create a new clientspec
# Copy the tarball from shared storage
tar xfz trunk-nightly.tgz
cd trunk
o4 fstat .@<cl> --changed $(cat .o4/changelist) |
  manifold -m 10485760 o4 drop --checksum |
  gatling o4  pyforce sync -f |
  manifold -m 10485760 o4 drop --checksum |
  of fail
```

This all works because the `.o4` directory from trunk was also rolled
into the tarball.


# Server-client protocol

The client determines what is needed to reach the target changelist.
It starts with the local file cache, adds anything that's missing from
the fstat server. If the fstat server redirects to a different
changelist, the client will fill the difference between the fstat
redirect changelist and the target from perforce.

The server may redirect to a different changelist as long as the new
changelist is less than the demanded target changelist.

This allows the server to skip changelists if there are performance or
space concerns on the server, without disrupting the client.

It also means the client can operate when the server is down for
maintenace. As long as perforce is equipped to handle the increase in
traffic.

# Copyright

Helix core, P4 and Perforce are registered trademarks owned by
Perforce Software, Inc.

## o4 license and copyright

Copyright (c) 2018, salesforce.com, inc. All rights reserved.

SPDX-License-Identifier: BSD-3-Clause

For full license text, see the license.txt file in the repo root or https://opensource.org/licenses/BSD-3-Clause
