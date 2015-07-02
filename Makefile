REBAR=./rebar3
.DEFAULT_GOAL := compile

.PHONY: compile
compile:
	$(REBAR) compile | sed -e 's|_build/default/lib/turtle/||'

.PHONY: dialyzer
dialyzer:
	$(REBAR) dialyzer | sed -e 's|_build/default/lib/turtle/||'

.PHONY: test	
test:
	$(REBAR) ct | sed -e 's|_build/test/lib/turtle/||'

## Travis test is like the `test` target, but it doesn't use sed(1) to simplify the
## output. This is important since we want to capture a failing test case by
## looking at the exit code of the program.
.PHONY: travis-test
travis-test:
	$(REBAR) ct

.PHONY: update
update:
	$(REBAR) update

