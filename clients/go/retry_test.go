package fibril

import "testing"

func TestRetryClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want RetryAdvice
	}{
		{"disconnect", &DisconnectionError{Message: "x"}, RetryRetry},
		{"broken pipe", &BrokenPipeError{Message: "x"}, RetryRetry},
		{"redirect", &RedirectError{}, RetryRetry},
		{"not owner 409", &ServerError{Code: 409}, RetryRetry},
		{"server 500", &ServerError{Code: 500}, RetryRetry},
		{"not found 404", &ServerError{Code: 404}, RetryDoNotRetry},
		{"invalid 400", &ServerError{Code: 400}, RetryDoNotRetry},
		{"auth 401", &ServerError{Code: 401}, RetryDoNotRetry},
		{"local serialize", &SerializationError{Message: "x"}, RetryDoNotRetry},
	}
	for _, c := range cases {
		if got := AdviseRetry(c.err); got != c.want {
			t.Errorf("%s: AdviseRetry = %q, want %q", c.name, got, c.want)
		}
		if got := IsRetryable(c.err); got != (c.want == RetryRetry) {
			t.Errorf("%s: IsRetryable = %v", c.name, got)
		}
	}
}
