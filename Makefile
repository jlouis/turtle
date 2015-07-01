REBAR=./rebar3

.PHONY: update
update:
	$(REBAR) update

.PHONY: compile
compile:
	$(REBAR) compile | sed -e 's|_build/default/lib/turtle/||'

.PHONY: dialyzer
dialyzer:
	$(REBAR) dialyzer | sed -e 's|_build/default/lib/turtle/||'

.PHONY: test	
test:
	$(REBAR) ct


