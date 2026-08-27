package service

// TODO: REVIEW GONVOKOD

// import (
// 	"context"
// 	"file-cipher-core/internal/entity"

// 	"golang.org/x/sync/errgroup"
// )

// type flusher interface {
// 	Run(ctx context.Context, in <-chan entity.FlushItem) error
// }

// func runPipeline[J any](ctx context.Context, f flusher, workers int, produce func(ctx context.Context, jobs chan<- J) error,
// 	work func(ctx context.Context, jobs <-chan J, items chan<- entity.FlushItem) error) error {

// 	g, gctx := errgroup.WithContext(ctx)
// 	jobs := make(chan J, workers)
// 	items := make(chan entity.FlushItem, workers)

// 	g.Go(func() error {
// 		return f.Run(gctx, items)
// 	})

// 	g.Go(func() error {
// 		wg, wctx := errgroup.WithContext(gctx)
// 		for i := 0; i < workers; i++ {
// 			wg.Go(func() error {
// 				return work(wctx, jobs, items)
// 			})
// 		}
// 		err := wg.Wait()
// 		close(items)
// 		return err
// 	})

// 	g.Go(func() error {
// 		defer close(jobs)
// 		return produce(gctx, jobs)
// 	})

// 	return g.Wait()
// }
