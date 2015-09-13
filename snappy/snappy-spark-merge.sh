#!/bin/sh

git fetch origin -v
git fetch upstream -v
git checkout snappy/master
git rebase origin/snappy/master
git merge upstream/master "$@"
