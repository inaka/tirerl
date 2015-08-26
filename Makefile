PROJECT = tirerl

DEPS = jiffy shotgun worker_pool
SHELL_DEPS = sync

dep_sync = git git://github.com/rustyio/sync.git 475d728a
dep_jiffy = git https://github.com/davisp/jiffy.git 0.14.2
dep_shotgun = git https://github.com/inaka/shotgun.git 0.1.12
dep_worker_pool = git https://github.com/inaka/worker_pool 1.0.3

DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Wrace_conditions

include erlang.mk

CT_SUITES = root_handler
CT_OPTS = -erl_args -config rel/sys.config

SHELL_OPTS = -name ${PROJECT}@`hostname` -s ${PROJECT} -s sync -config rel/sys.config
