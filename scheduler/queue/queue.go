package queue

type SchedulingQueue struct{}

func New() *SchedulingQueue {
	return &SchedulingQueue{}
}
