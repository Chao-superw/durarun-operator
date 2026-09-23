package runtime

import "context"

// Context is the handle passed to job functions, giving access to the
// underlying context.Context and the Step method.
type Context struct {
	ctx     context.Context
	runtime *Runtime
}

// Ctx returns the underlying context.Context.
func (c *Context) Ctx() context.Context { return c.ctx }
