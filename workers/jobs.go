package workers

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
)

type Job func(ctx context.Context) error

type indexedJob struct {
	run   Job
	index int
}

type indexedJobResult struct {
	err   error
	index int
}

type ReversibleJob interface {
	Run(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type ReversibleJobResult struct {
	RunErr      error
	RollbackErr error
}

func RunJobsAtomically(runCtx context.Context, rbCtx context.Context, trxs []ReversibleJob, workers int) (results []ReversibleJobResult, err error) {
	if trxs == nil {
		return nil, nil
	}

	results = make([]ReversibleJobResult, 0, len(trxs))

	runJobs := make([]Job, 0, len(trxs))
	for i := range trxs {
		trx := trxs[i]
		runJobs = append(runJobs, trx.Run)
	}
	cancellableRunCtx, cancelRunCtx := context.WithCancel(runCtx)
	defer cancelRunCtx()
	runErrs := RunJobs(cancellableRunCtx, runJobs, workers)

	rollbackJobs := make([]Job, 0, len(trxs))
	rollbackJobIndexes := make([]int, 0, len(trxs))
	rollback := false
	for i := range trxs {
		trx := trxs[i]
		runErr := runErrs[i]
		results = append(results, ReversibleJobResult{
			RunErr:      runErr,
			RollbackErr: nil,
		})
		if runErr == nil {
			rollbackJobs = append(rollbackJobs, trx.Rollback)
			rollbackJobIndexes = append(rollbackJobIndexes, i)
		} else {
			rollback = true
		}
	}

	if !rollback {
		return results, nil
	}

	cancellableRbCtx, cancelRbCtx := context.WithCancel(rbCtx)
	defer cancelRbCtx()
	rollbackErrs := RunJobs(cancellableRbCtx, rollbackJobs, workers)
	for i := range rollbackErrs {
		results[rollbackJobIndexes[i]].RollbackErr = rollbackErrs[i]
	}
	return results, fmt.Errorf("one or more jobs failed")
}

func RunJobs(ctx context.Context, jobs []Job, workers int) (results []error) {
	if jobs == nil {
		return nil
	} else if len(jobs) == 0 {
		return make([]error, 0)
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if workers > len(jobs) {
		workers = len(jobs)
	}

	results = make([]error, len(jobs))
	resultsChan := make(chan indexedJobResult, len(jobs))
	jobChan := make(chan indexedJob, len(jobs))
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for job := range jobChan {
			select {
			case <-ctx.Done():
				resultsChan <- indexedJobResult{
					index: job.index,
					err:   ctx.Err(),
				}
			default:
				var err error
				func() {
					defer func() {
						if r := recover(); r != nil {
							err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
						}
					}()
					err = job.run(ctx)
				}()
				resultsChan <- indexedJobResult{
					index: job.index,
					err:   err,
				}
			}
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	for i := range jobs {
		jobChan <- indexedJob{
			run:   jobs[i],
			index: i,
		}
	}
	close(jobChan)

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	for result := range resultsChan {
		results[result.index] = result.err
	}

	return results
}
