package rapid

// Logger abstracts the std log interface. There's no standard
// interface, this is the closest we get, unfortunately.
type Logger interface {
	Print(...interface{})
	Printf(string, ...interface{})
	Println(...interface{})

	Fatal(...interface{})
	Fatalf(string, ...interface{})
	Fatalln(...interface{})
}

// NOOPLogger log to a black hole
var NOOPLogger Logger = &noopLogger{}

type noopLogger struct{}

func (n *noopLogger) Print(args ...interface{})         {}
func (n *noopLogger) Printf(_ string, _ ...interface{}) {}
func (n *noopLogger) Println(_ ...interface{})          {}
func (n *noopLogger) Fatal(_ ...interface{})            {}
func (n *noopLogger) Fatalf(_ string, _ ...interface{}) {}
func (n *noopLogger) Fatalln(_ ...interface{})          {}
