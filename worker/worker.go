package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

	"cube/task"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

type Worker struct {
	Name      string
	Queue     *queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
}

var stateTransitionMap = map[task.State][]task.State{
	task.Pending: {
		task.Scheduled,
	},

	task.Scheduled: {
		task.Scheduled,
		task.Running,
		task.Failed,
	},

	task.Running: {
		task.Running,
		task.Completed,
		task.Failed,
	},

	task.Completed: {},

	task.Failed: {},
}

func Contains(states []task.State, state task.State) bool {
	for _, s := range states {
		if s == state {
			return true
		}
	}

	return false
}

func ValidStateTransition(src task.State, dst task.State) bool {
	return Contains(stateTransitionMap[src], dst)
}

func (w *Worker) CollectStats() {
	fmt.Println("I will collect stats")
}

func (w *Worker) AddTask(t *task.Task) {
	w.Queue.Enqueue(t)
	w.TaskCount++

	log.Printf("Task %v added to the queue\n", t.ID)
}

func (w *Worker) RunTask() task.DockerResult {
	t := w.Queue.Dequeue()

	if t == nil {
		log.Println("No task in the queue")
		return task.DockerResult{
			Error: nil,
		}
	}

	taskQueued, ok := t.(*task.Task)
	if !ok {
		return task.DockerResult{
			Error: errors.New("invalid task type in queue"),
		}
	}

	taskPersisted := w.Db[taskQueued.ID]

	if taskPersisted == nil {
		log.Printf("Task %v not found in database\n", taskQueued.ID)

		taskPersisted = taskQueued
		w.Db[taskQueued.ID] = taskQueued
	}

	var result task.DockerResult

	if ValidStateTransition(
		taskPersisted.State,
		taskQueued.State,
	) {
		switch taskQueued.State {

		case task.Scheduled:
			result = w.StartTask(taskQueued)

		case task.Completed:
			result = w.StopTask(taskQueued)

		default:
			result.Error = errors.New("unexpected task state")
		}

	} else {
		result.Error = fmt.Errorf(
			"invalid transition from %v to %v",
			taskPersisted.State,
			taskQueued.State,
		)
	}

	return result
}

func (w *Worker) StartTask(t *task.Task) task.DockerResult {
	now := time.Now().UTC()
	t.StartTime = &now

	config := task.NewConfig(t)

	d, err := task.NewDocker(config)

	if err != nil {
		log.Printf(
			"Error creating docker client for task %v\n",
			t.ID,
		)

		t.State = task.Failed
		w.Db[t.ID] = t

		return task.DockerResult{
			Error: errors.New("failed to create docker client"),
		}
	}

	result := d.Run()

	if result.Error != nil {
		log.Printf(
			"Error running task %v: %v\n",
			t.ID,
			result.Error,
		)

		t.State = task.Failed
		w.Db[t.ID] = t

		return result
	}

	t.ContainerID = result.ContainerId
	t.State = task.Running

	w.Db[t.ID] = t

	return result
}

func (w *Worker) GetTasks() []*task.Task {
    tasks := make([]*task.Task, 0, len(w.Db))

    for _, t := range w.Db {
        tasks = append(tasks, t)
    }

    return tasks
}

func (w *Worker) StopTask(t *task.Task) task.DockerResult {
	config := task.NewConfig(t)

	d, err := task.NewDocker(config)

	if err != nil {
		log.Printf(
			"Error creating docker client for task %v\n",
			t.ID,
		)

		t.State = task.Failed
		w.Db[t.ID] = t

		return task.DockerResult{
			Error: errors.New("failed to create docker client"),
		}
	}

	result := d.Stop(t.ContainerID)

	if result.Error != nil {
		log.Printf(
			"Error stopping container %v: %v\n",
			t.ContainerID,
			result.Error,
		)

		return result
	}

	now := time.Now().UTC()
	t.FinishTime = &now
	t.State = task.Completed

	w.Db[t.ID] = t

	log.Printf(
		"Stopped and removed container %v for task %v\n",
		t.ContainerID,
		t.ID,
	)

	return result
}