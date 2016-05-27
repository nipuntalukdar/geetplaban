package geetplaban

import (
	"math/rand"
	"testing"
	"time"
)

func TestStats(t *testing.T) {

	stsm := NewAllStats()
	go func(sts *AllStats) {
		for {
			sts.addFailed(1, 1)
			sts.addFailed(uint(rand.Intn(150)), 1)
			sts.addFailed(uint(rand.Intn(150)), 1)
			sts.addEmitted(uint(rand.Intn(150)), 2)
			sts.addFailed(uint(rand.Intn(150)), 1)
			sts.addTimespent(uint(rand.Intn(150)), 10)
		}
	}(stsm)
	time.Sleep(65 * time.Second)
	stsm.Stop()
}
