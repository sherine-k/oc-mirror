package transport

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/containers/image/v5/docker/reference"
	"github.com/containers/image/v5/pkg/sysregistriesv2"
	cimagetypesv5 "github.com/containers/image/v5/types"
	"github.com/openshift/oc-mirror/v2/internal/pkg/log"
)

type Mirror struct {
	OriginUrl       string
	MirrorEndpoints []MirrorEndpoint
}

type MirrorEndpoint struct {
	Endpoint string
	Secure   bool
}
type mirroredTransport struct {
	inner   http.RoundTripper
	mirrors []Mirror
	logger  log.PluggableLoggerInterface
}

var _ http.RoundTripper = (*mirroredTransport)(nil)

func NewWithMirrors(inner http.RoundTripper, mirrors []Mirror, logger log.PluggableLoggerInterface) http.RoundTripper {
	return &mirroredTransport{
		inner:   inner,
		mirrors: mirrors,
		logger:  logger,
	}
}

func (t *mirroredTransport) RoundTrip(in *http.Request) (out *http.Response, err error) {
	if len(t.mirrors) > 0 {
		for _, mirror := range t.mirrors {
			if isApplicable, err := mirror.isApplicableTo(*in.URL); isApplicable && err == nil {
				t.logger.Debug("Request %v: mirror %v matches\n", *in.URL, mirror)

				for _, endpoint := range mirror.MirrorEndpoints {
					mirroredRequest, err := mirror.useMirrorEndpoint(in, endpoint)
					if err != nil {
						t.logger.Debug("ERROR: Request %v: %v\n", mirroredRequest, err)
						continue
					}
					out, err = t.inner.RoundTrip(mirroredRequest)
					if err != nil {
						t.logger.Debug("ERROR: Request %v: %v\n", mirroredRequest, err)
						continue
					}
					return out, err
				}
			}
		}
	}
	return t.inner.RoundTrip(in)
}

func (m Mirror) isApplicableTo(url url.URL) (bool, error) {
	mirrorUrl, err := url.Parse(m.OriginUrl)
	if err != nil {
		return false, fmt.Errorf("unable to parse mirror origin url %s: %v", m.OriginUrl, err)
	}
	if strings.Contains(url.Host, mirrorUrl.Host) || strings.Contains(url.Path, mirrorUrl.Path) {
		return true, nil
	}
	return false, nil
}

func (m Mirror) useMirrorEndpoint(in *http.Request, mirrorEndpoint MirrorEndpoint) (*http.Request, error) {
	originURL := m.OriginUrl
	if !strings.HasPrefix(m.OriginUrl, "http") {
		originURL = "https://" + originURL
	}
	mirrorUrl, err := url.Parse(originURL)
	if err != nil {
		return in, fmt.Errorf("unable to parse mirror origin url %s: %v", m.OriginUrl, err)
	}
	endpointURL := mirrorEndpoint.Endpoint
	if !strings.HasPrefix(m.OriginUrl, "http") {
		if mirrorEndpoint.Secure {
			endpointURL = "https://" + endpointURL
		} else {
			endpointURL = "http://" + endpointURL
		}
	}
	mirrorEndpointUrl, err := url.Parse(endpointURL)
	if err != nil {
		return in, fmt.Errorf("unable to parse mirror endpoint %s: %v", mirrorEndpoint.Endpoint, err)
	}

	// mirroredIn := in.Clone(in.Context())
	inURL := in.URL.String()
	inURL = strings.Replace(inURL, mirrorUrl.Host, mirrorEndpointUrl.Host, 1)
	inURL = strings.Replace(inURL, mirrorUrl.Path, mirrorEndpointUrl.Path, 1)
	if in.URL.Scheme == "https" && !mirrorEndpoint.Secure {
		inURL = strings.Replace(inURL, "https:", "http:", 1)
	}
	if in.URL.Scheme == "http" && mirrorEndpoint.Secure {
		inURL = strings.Replace(inURL, "http:", "https:", 1)
	}
	mirroredRequestURL, err := url.Parse(inURL)
	if err != nil {
		return in, fmt.Errorf("unable to parse mirror endpoint %s: %v", mirrorEndpoint.Endpoint, err)

	}
	// mirroredIn.URL = mirroredRequestURL
	in.URL = mirroredRequestURL
	// fmt.Printf("using %v as mirror of %v\n", mirroredIn.URL.String(), in.URL.String())
	fmt.Printf("using %v as mirror of %v\n", mirroredRequestURL, inURL)
	// return mirroredIn, nil
	return in, nil
}

func FindMirrors(sysCtx *cimagetypesv5.SystemContext, imgRef string, logger log.PluggableLoggerInterface) []Mirror {
	m := make([]Mirror, 1)
	ref, err := reference.ParseNormalizedNamed(imgRef)
	if err != nil {
		logger.Warn("Unable to parse %s: %v", imgRef, err)
		return []Mirror{}
	}
	reg, err := sysregistriesv2.FindRegistry(sysCtx, imgRef)
	if err != nil {
		logger.Warn("Cannot find registry for %s: %v", imgRef, err)
		return []Mirror{}
	}
	if reg != nil {
		pullSources, err := reg.PullSourcesFromReference(ref)
		if err != nil {
			logger.Warn("Cannot find mirrors for %s: %v", imgRef, err)
			return []Mirror{}
		}
		m[0] = Mirror{
			OriginUrl:       imgRef,
			MirrorEndpoints: make([]MirrorEndpoint, len(pullSources)),
		}
		for i, p := range pullSources {
			m[0].MirrorEndpoints[i] = MirrorEndpoint{
				Endpoint: p.Endpoint.Location,
				Secure:   !p.Endpoint.Insecure,
			}
		}
		return m
	}
	return []Mirror{}
}
