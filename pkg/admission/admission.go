package admission

// type Priority uint16

// func NewPriority(business, user uint8) Priority {
// 	return (Priority(business) << 8) & Priority(user)
// }

// type Request interface{}

// // Quota contains implementation specific information about the quota allocated
// // to a given acquisition. Quota should carry an implementation specific method
// // to get a handle on a response.
// type Quota interface{}

// type Response interface{}

// type Report interface {
// }

// // Ticket might maintain some useful information like when the request was
// // admitted
// type Ticket interface {
// }

// type RateKeeper interface {
// 	Tick(context.Context, time.Time)
// 	Acquire(context.Context, Request) (Quota, bool)
// 	Release(context.Context, Response)
// 	Report(context.Context, Report)
// }

// type ControllerConfig struct {
// 	TickRate   time.Duration
// 	RateKeeper RateKeeper
// }

// type controllerI interface {
// 	Request(context.Context, Priority, Request) (Response, bool)
// 	Release(context.Context, Priority, Response)
// }

// // Controller deals with queuing and admission control decisions for a layer.
// // A Controller utilizes a RateKeeper.
// type Controller struct {
// 	// We need some sort of queues
// 	// Maybe we just hide those as stack-local state in some processing goroutine.

// }
