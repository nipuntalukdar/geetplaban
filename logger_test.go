package geetplaban

import (
	"testing"
	"time"
)

func TestLogger(t *testing.T) {
	for i := 0; i < 80000; i++ {
		LOG.Infof("Helllo")
	}
	time.Sleep(10 * time.Second)
}
