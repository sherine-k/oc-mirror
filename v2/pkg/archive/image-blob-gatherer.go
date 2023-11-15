package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/containers/image/v5/manifest"
	"github.com/containers/image/v5/transports/alltransports"
	"github.com/containers/image/v5/types"
	imagespecv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/openshift/oc-mirror/v2/pkg/mirror"
)

type ImageBlobGatherer struct {
	ctx  context.Context
	opts *mirror.CopyOptions
}

func NewImageBlobGatherer(ctx context.Context, opts *mirror.CopyOptions) *ImageBlobGatherer {
	return &ImageBlobGatherer{
		ctx:  ctx,
		opts: opts,
	}
}
func (o *ImageBlobGatherer) GatherBlobs(imgRef string) (blobs []string, retErr error) {
	blobs = []string{}
	o.opts.DeprecatedTLSVerify.WarnIfUsed([]string{"--src-tls-verify", "--dest-tls-verify"})
	o.opts.All = true
	o.opts.RemoveSignatures, _ = strconv.ParseBool("true")

	if err := mirror.ReexecIfNecessaryForImages([]string{imgRef}...); err != nil {
		return blobs, err
	}

	// policyContext, err := o.opts.Global.GetPolicyContext()
	// if err != nil {
	// 	return blobs, fmt.Errorf("error loading trust policy: %v", err)
	// }
	// defer func() {
	// 	if err := policyContext.Destroy(); err != nil {
	// 		retErr = mirror.NoteCloseFailure(retErr, "tearing down policy context", err)
	// 	}
	// }()

	srcRef, err := alltransports.ParseImageName(imgRef)
	if err != nil {
		return blobs, fmt.Errorf("invalid source name %s: %v", imgRef, err)
	}
	sourceCtx, err := o.opts.SrcImage.NewSystemContext()
	if err != nil {
		return blobs, err
	}
	img, err := srcRef.NewImageSource(o.ctx, sourceCtx)
	if err != nil {
		return blobs, err
	}
	// TODO add the image digest (manifest digest)
	// blobs = append(blobs, )

	manifestBytes, mime, err := img.GetManifest(o.ctx, nil)
	if err != nil {
		return blobs, err
	}
	switch mime {
	case imagespecv1.MediaTypeImageIndex:
		indexBlobs, err := o.getBlobsOfIndex(img, manifestBytes)
		if err != nil {
			return blobs, err
		}
		blobs = append(blobs, indexBlobs...)
	case imagespecv1.MediaTypeImageManifest:
		ociManifestBlobs, err := o.getBlobsOfOciManifest(manifestBytes)
		if err != nil {
			return blobs, err
		}
		blobs = append(blobs, ociManifestBlobs...)
	case manifest.DockerV2ListMediaType:
		listBlobs, err := o.getBlobsOfManifestList(img, manifestBytes)
		if err != nil {
			return blobs, err
		}
		blobs = append(blobs, listBlobs...)
	case manifest.DockerV2Schema2MediaType:
		dockerManifestBlobs, err := o.getBlobsOfDockerManifest(manifestBytes)
		if err != nil {
			return blobs, err
		}
		blobs = append(blobs, dockerManifestBlobs...)

	}
	return blobs, nil
}

func (o *ImageBlobGatherer) getBlobsOfOciManifest(manifestBytes []byte) ([]string, error) {
	var ociManifest imagespecv1.Manifest
	if err := json.Unmarshal(manifestBytes, &ociManifest); err != nil {
		return nil, fmt.Errorf("error unmarshalling manifest: %v", err)
	}
	blobs := []string{}
	for _, layer := range ociManifest.Layers {
		blobs = append(blobs, layer.Digest.String())
	}
	blobs = append(blobs, ociManifest.Config.Digest.String())
	return blobs, nil
}

func (o *ImageBlobGatherer) getBlobsOfDockerManifest(manifestBytes []byte) ([]string, error) {
	dockerManifest, err := manifest.Schema2FromManifest(manifestBytes)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling manifest: %v", err)
	}
	blobs := []string{}
	for _, layer := range dockerManifest.LayerInfos() {
		blobs = append(blobs, layer.Digest.String())
	}
	blobs = append(blobs, dockerManifest.ConfigInfo().Digest.String())
	return blobs, nil
}

func (o *ImageBlobGatherer) getBlobsOfIndex(img types.ImageSource, manifestBytes []byte) ([]string, error) {
	ociIndex, err := manifest.OCI1IndexFromManifest(manifestBytes)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling manifest: %v", err)
	}
	blobs := []string{}
	for _, aManifest := range ociIndex.Manifests {
		blobs = append(blobs, aManifest.Digest.String())
		switch aManifest.MediaType {
		case imagespecv1.MediaTypeImageIndex:
			indexBlobs, err := o.getBlobsOfIndex(img, aManifest.Data)
			if err != nil {
				return blobs, err
			}
			blobs = append(blobs, indexBlobs...)
		case imagespecv1.MediaTypeImageManifest:
			ociManifestBlobs, err := o.getBlobsOfOciManifest(aManifest.Data)
			if err != nil {
				return blobs, err
			}
			blobs = append(blobs, ociManifestBlobs...)
		case manifest.DockerV2ListMediaType:
			listBlobs, err := o.getBlobsOfManifestList(img, aManifest.Data)
			if err != nil {
				return blobs, err
			}
			blobs = append(blobs, listBlobs...)
		case manifest.DockerV2Schema2MediaType:
			dockerManifestBlobs, err := o.getBlobsOfDockerManifest(aManifest.Data)
			if err != nil {
				return blobs, err
			}
			blobs = append(blobs, dockerManifestBlobs...)

		}
	}

	return blobs, nil
}

func (o *ImageBlobGatherer) getBlobsOfManifestList(img types.ImageSource, manifestBytes []byte) ([]string, error) {
	list, err := manifest.Schema2ListFromManifest(manifestBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing schema2 manifest list: %w", err)
	}

	blobs := []string{}
	for _, aManifest := range list.Manifests {
		blobs = append(blobs, aManifest.Digest.String())

		imgManifestBytes, mimeType, err := img.GetManifest(o.ctx, &aManifest.Digest)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling manifest: %v", err)
		}

		switch mimeType {
		case imagespecv1.MediaTypeImageIndex:
			indexBlobs, err := o.getBlobsOfIndex(img, imgManifestBytes)
			if err != nil {
				return blobs, err
			}
			blobs = append(blobs, indexBlobs...)
		case imagespecv1.MediaTypeImageManifest:
			ociManifestBlobs, err := o.getBlobsOfOciManifest(imgManifestBytes)
			if err != nil {
				return blobs, err
			}
			blobs = append(blobs, ociManifestBlobs...)
		case manifest.DockerV2ListMediaType:
			listBlobs, err := o.getBlobsOfManifestList(img, imgManifestBytes)
			if err != nil {
				return blobs, err
			}
			blobs = append(blobs, listBlobs...)
		case manifest.DockerV2Schema2MediaType:
			dockerManifestBlobs, err := o.getBlobsOfDockerManifest(imgManifestBytes)
			if err != nil {
				return blobs, err
			}
			blobs = append(blobs, dockerManifestBlobs...)

		}
	}
	return blobs, nil
}
