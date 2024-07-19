#!/bin/bash
. /usr/local/etc/fixinventory/defaults
. /usr/local/fix-venv-python3/bin/activate

# dispatch to the correct console script
case $1 in
  collect)
    shift
    exec collect_single "$@"
    ;;
  post-collect)
    shift
    exec post_collect "$@"
    ;;
  *)
    # backward compatibility: delegate to collect_single
    exec collect_single "$@"
    ;;
esac

