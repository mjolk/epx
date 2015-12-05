package util

import (
	"math/rand"
)

const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 abcdefghijklmnopqrstuvwxyz" +
	"~!@#$%^&*()-_+={}[]\\|<,>.?/\"';:`"

const Maxlen = 10

func RandStrings(N int) []string {
	r := make([]string, N)
	ri := 0
	buf := make([]byte, Maxlen)
	known := map[string]bool{}

	for i := 0; i < N; i++ {
	retry:
		l := rand.Intn(Maxlen)
		for j := 0; j < l; j++ {
			buf[j] = chars[rand.Intn(len(chars))]
		}
		s := string(buf[0:l])
		if known[s] {
			goto retry
		}
		known[s] = true
		r[ri] = s
		ri++
	}
	return r
}
