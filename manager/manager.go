package manager

import (
    "cube/task"
    "fmt"
	"bytes"
    "encoding/json"
	"log"
    "net/http"

	"cube/worker"

    "github.com/golang-collections/collections/queue"
    "github.com/google/uuid"
)

type Manager struct {
    Pending       *queue.Queue
    TaskDb        map[uuid.UUID]*task.Task
    EventDb       map[string][]*task.TaskEvent
    Workers       []string
    WorkerTaskMap map[string][]uuid.UUID
    TaskWorkerMap map[uuid.UUID]string
    LastWorker    int
}

func (m *Manager) SelectWorker() string {
    var newWorker int
	if m.LastWorker+1 < len(m.Workers) {
		newWorker = m.LastWorker + 1
		m.LastWorker++
	} else {
		newWorker = 0
		m.LastWorker = 0
	}

	return m.Workers[newWorker]
}

func (m *Manager) UpdateTasks() {
	for _, worker := range m.Workers {
        log.Printf("Checking worker %v for task updates", worker)
        url := fmt.Sprintf("http://%s/tasks", worker)
        resp, err := http.Get(url)
        if err != nil {
            log.Printf("Error connecting to %v: %v\n", worker, err)
        }

        if resp.StatusCode != http.StatusOK {
            log.Printf("Error sending request: %v\n", err)
        }

        d := json.NewDecoder(resp.Body)
        var tasks []*task.Task
        err = d.Decode(&tasks)
        if err != nil {
            log.Printf("Error unmarshalling tasks: %s\n", err.Error())
        }

		for _, t := range tasks {
            log.Printf("Attempting to update task %v\n", t.ID)

            _, ok := m.TaskDb[t.ID]
            if !ok {
                log.Printf("Task with ID %s not found\n", t.ID)
                return
            }

            if m.TaskDb[t.ID].State != t.State {
                m.TaskDb[t.ID].State = t.State
            }

            m.TaskDb[t.ID].StartTime = t.StartTime
            m.TaskDb[t.ID].FinishTime = t.FinishTime
            m.TaskDb[t.ID].ContainerID = t.ContainerID
        }
	}
}

func (m *Manager) SendWork() {
    if m.Pending.Len() == 0 {
        log.Println("No work in the queue")
        return
    }

    selectedWorker := m.SelectWorker()

    e := m.Pending.Dequeue()
    te, ok := e.(*task.TaskEvent)
    if !ok {
        log.Println("Invalid task event in queue")
        return
    }

    t := te.Task

    m.EventDb[te.ID.String()] = append(
        m.EventDb[te.ID.String()],
        te,
    )

    m.WorkerTaskMap[selectedWorker] = append(
        m.WorkerTaskMap[selectedWorker],
        t.ID,
    )
    m.TaskWorkerMap[t.ID] = selectedWorker
    m.TaskDb[t.ID] = t

    data, err := json.Marshal(te)
    if err != nil {
        log.Printf("Unable to marshal task event: %v", err)
        m.Pending.Enqueue(te)
        return
    }

    url := fmt.Sprintf("http://%s/tasks", selectedWorker)
    resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
    if err != nil {
        log.Printf("Error connecting to %s: %v", selectedWorker, err)
        m.Pending.Enqueue(te)
        return
    }
    defer resp.Body.Close()

    d := json.NewDecoder(resp.Body)

    if resp.StatusCode != http.StatusCreated {
        var e worker.ErrResponse
        if err := d.Decode(&e); err != nil {
            log.Printf("Error decoding error response: %v", err)
            return
        }

        log.Printf("Response error (%d): %s", e.HTTPStatusCode, e.Message)
        return
    }

    if err := d.Decode(t); err != nil {
        log.Printf("Error decoding task response: %v", err)
        return
    }

    log.Printf("Task submitted: %#v", t)
}

func (m *Manager) AddTask(te *task.TaskEvent) {
    m.Pending.Enqueue(te)
}

func New(workers []string) *Manager {
    taskDb := make(map[uuid.UUID]*task.Task)
    eventDb := make(map[string][]*task.TaskEvent)
    workerTaskMap := make(map[string][]uuid.UUID)
    taskWorkerMap := make(map[uuid.UUID]string)

    for _, worker := range workers {
        workerTaskMap[worker] = []uuid.UUID{}
    }

    return &Manager{
        Pending:       queue.New(),
        Workers:       workers,
        TaskDb:        taskDb,
        EventDb:       eventDb,
        WorkerTaskMap: workerTaskMap,
        TaskWorkerMap: taskWorkerMap,
    }
}