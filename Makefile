REBAR=./rebar3
.DEFAULT_GOAL := compile

.PHONY: compile
compile:
	$(REBAR) compile

.PHONY: dialyzer
dialyzer:
	$(REBAR) dialyzer

.PHONY: test	
test:
	$(REBAR) ct

.PHONY: edoc
edoc:
	$(REBAR) edoc

.PHONY: update
update:
	$(REBAR) update

