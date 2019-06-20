package radix

import (
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialUseTLS(t *testing.T) {
	// In order to test a TLS connection we need to start a TLS terminating proxy

	// The following are taken from crypto/tls/tls_test.go

	// This function is used to avoid static code analysis from identifying the private key
	testingKey := func(s string) string { return strings.Replace(s, "TESTING KEY", "PRIVATE KEY", 2) }

	var rsaCertPEM = `-----BEGIN CERTIFICATE-----
MIIB0zCCAX2gAwIBAgIJAI/M7BYjwB+uMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTIwOTEyMjE1MjAyWhcNMTUwOTEyMjE1MjAyWjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBANLJ
hPHhITqQbPklG3ibCVxwGMRfp/v4XqhfdQHdcVfHap6NQ5Wok/4xIA+ui35/MmNa
rtNuC+BdZ1tMuVCPFZcCAwEAAaNQME4wHQYDVR0OBBYEFJvKs8RfJaXTH08W+SGv
zQyKn0H8MB8GA1UdIwQYMBaAFJvKs8RfJaXTH08W+SGvzQyKn0H8MAwGA1UdEwQF
MAMBAf8wDQYJKoZIhvcNAQEFBQADQQBJlffJHybjDGxRMqaRmDhX0+6v02TUKZsW
r5QuVbpQhH6u+0UgcW0jp9QwpxoPTLTWGXEWBBBurxFwiCBhkQ+V
-----END CERTIFICATE-----
`

	var rsaKeyPEM = testingKey(`-----BEGIN RSA TESTING KEY-----
MIIBOwIBAAJBANLJhPHhITqQbPklG3ibCVxwGMRfp/v4XqhfdQHdcVfHap6NQ5Wo
k/4xIA+ui35/MmNartNuC+BdZ1tMuVCPFZcCAwEAAQJAEJ2N+zsR0Xn8/Q6twa4G
6OB1M1WO+k+ztnX/1SvNeWu8D6GImtupLTYgjZcHufykj09jiHmjHx8u8ZZB/o1N
MQIhAPW+eyZo7ay3lMz1V01WVjNKK9QSn1MJlb06h/LuYv9FAiEA25WPedKgVyCW
SmUwbPw8fnTcpqDWE3yTO3vKcebqMSsCIBF3UmVue8YU3jybC3NxuXq3wNm34R8T
xVLHwDXh/6NJAiEAl2oHGGLz64BuAfjKrqwz7qMYr9HCLIe/YsoWq/olzScCIQDi
D2lWusoe2/nEqfDVVWGWlyJ7yOmqaVm/iNUN9B2N2g==
-----END RSA TESTING KEY-----
`)
	pem := []byte(rsaCertPEM + rsaKeyPEM)
	cert, err := tls.X509KeyPair(pem, pem)
	require.NoError(t, err)

	// The following TLS proxy is based on https://gist.github.com/cs8425/a742349a55596f1b251a#file-tls2tcp_server-go
	listener, err := tls.Listen("tcp", ":63790", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	require.NoError(t, err)
	// Used to prevent a race during shutdown failing the test
	m := sync.Mutex{}
	shuttingDown := false
	defer func() {
		m.Lock()
		shuttingDown = true
		m.Unlock()
		listener.Close()
	}()

	// Dials 127.0.0.1:6379 and proxies traffic
	proxyConnection := func(lConn net.Conn) {
		defer lConn.Close()

		rConn, err := net.DialTCP("tcp", nil, &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 6379,
		})
		require.NoError(t, err)
		defer rConn.Close()

		chanFromConn := func(conn net.Conn) chan []byte {
			c := make(chan []byte)

			go func() {
				b := make([]byte, 1024)

				for {
					n, err := conn.Read(b)
					if n > 0 {
						res := make([]byte, n)
						// Copy the buffer so it doesn't get changed while read by the recipient.
						copy(res, b[:n])
						c <- res
					}
					if err != nil {
						c <- nil
						break
					}
				}
			}()

			return c
		}

		lChan := chanFromConn(lConn)
		rChan := chanFromConn(rConn)

		for {
			select {
			case b1 := <-lChan:
				if b1 == nil {
					return
				}
				_, err = rConn.Write(b1)
				require.NoError(t, err)
			case b2 := <-rChan:
				if b2 == nil {
					return
				}
				_, err = lConn.Write(b2)
				require.NoError(t, err)
			}
		}

	}

	// Accept new connections
	go func() {
		for {
			lConn, err := listener.Accept()
			if err != nil {
				// Accept unblocks and returns an error after Shutdown is called on listener
				m.Lock()
				defer m.Unlock()
				if shuttingDown {
					// Exit
					break
				} else {
					require.NoError(t, err)
				}
			}
			go proxyConnection(lConn)
		}
	}()

	// Connect to the proxy, passing in an insecure flag as we are self-signed
	c, err := Dial("tcp", "127.0.0.1:63790", DialUseTLS(&tls.Config{
		InsecureSkipVerify: true,
	}))
	if err != nil {
		t.Fatal(err)
	} else if err := c.Do(Cmd(nil, "PING")); err != nil {
		t.Fatal(err)
	}

	// Confirm that the connection fails if verifying certificate
	_, err = Dial("tcp", "127.0.0.1:63790", DialUseTLS(nil), DialConnectTimeout(60*time.Minute))
	assert.Error(t, err)
}
