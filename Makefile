REBAR=rebar3

compile:
	$(REBAR) compile
	
test:
	$(REBAR) ct

