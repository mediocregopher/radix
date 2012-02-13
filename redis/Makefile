include $(GOROOT)/src/Make.inc

TARG=redis
GOFMT=gofmt -w -tabs -tabwidth=4

GOFILES=\
	doc.go\
	client.go\
	multi_command.go\
	urp.go\
	reply.go\
	utils.go\

include $(GOROOT)/src/Make.pkg

format:
	${GOFMT} ${GOFILES}
	${GOFMT} redis_test.go

