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
	$(REBAR) ct

.PHONY: update
update:
	$(REBAR) update

