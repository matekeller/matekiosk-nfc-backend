#!/bin/busybox ash
set -euo pipefail

# USAGE: $0 <key-name> <privkey-base64>

# initialize key
echo "-----BEGIN PRIVATE KEY-----" >~/.abuild/$1
echo "$2" >>~/.abuild/$1
echo "-----END PRIVATE KEY-----" >>~/.abuild/$1
cp alpine/$1.pub ~/.abuild/
sudo cp alpine/$1.pub /etc/apk/keys/

set -x

# initialize abuild config
sudo mkdir packages
echo "PACKAGER_PRIVKEY=~/.abuild/$1" >>~/.abuild/abuild.conf
echo "REPODEST=$PWD/packages" >>~/.abuild/abuild.conf
echo 'export JOBS=2' >>~/.abuild/abuild.conf
echo 'export MAKEFLAGS=-j$JOBS' >>~/.abuild/abuild.conf

# initialize abuild repos - otherwise weird errors appear
abuild-apk update
abuild-apk update --root $CBUILDROOT --arch $CTARGET

# make sure we own the relevant directory
sudo chown -R ${USER:-1000} alpine packages

# build the package
APKBUILD=alpine/APKBUILD abuild checksum
APKBUILD=alpine/APKBUILD abuild -r
