package delete

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/openshift/oc-mirror/v2/pkg/api/v1alpha2"
	"github.com/openshift/oc-mirror/v2/pkg/api/v1alpha3"
	"github.com/openshift/oc-mirror/v2/pkg/archive"
	"github.com/openshift/oc-mirror/v2/pkg/batch"
	clog "github.com/openshift/oc-mirror/v2/pkg/log"
	"github.com/openshift/oc-mirror/v2/pkg/manifest"
	"github.com/openshift/oc-mirror/v2/pkg/mirror"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

type DeleteImages struct {
	Log              clog.PluggableLoggerInterface
	Opts             mirror.CopyOptions
	Batch            batch.BatchInterface
	Blobs            archive.BlobsGatherer
	Config           v1alpha2.ImageSetConfiguration
	Manifest         manifest.ManifestInterface
	LocalStorageDisk string
	LocalStorageFQDN string
}

// WriteDeleteMetaData
func (o DeleteImages) WriteDeleteMetaData(images []v1alpha3.CopyImageSchema) error {
	o.Log.Info("writing delete metadata images to %s ", o.Opts.Global.WorkingDir+deleteDir)

	// we write the image and related blobs in yaml format to file for further processing
	filename := filepath.Join(o.Opts.Global.WorkingDir, deleteImagesYaml)
	discYamlFile := filepath.Join(o.Opts.Global.WorkingDir, discYaml)
	// used for versioning and comparing
	if len(o.Opts.Global.DeleteID) > 0 {
		filename = filepath.Join(o.Opts.Global.WorkingDir, strings.ReplaceAll(deleteImagesYaml, ".", "-"+o.Opts.Global.DeleteID+"."), "/")
		discYamlFile = filepath.Join(o.Opts.Global.WorkingDir, strings.ReplaceAll(discYaml, ".", "-"+o.Opts.Global.DeleteID+"."), "/")
	}
	// create the delete folder
	err := os.MkdirAll(o.Opts.Global.WorkingDir+deleteDir, 0755)
	if err != nil {
		o.Log.Error("%v ", err)
	}
	var items_map = make(map[string]v1alpha3.DeleteItem)

	// gather related blobs
	for _, img := range images {
		// copyIS, err := buildFormatedCopyImageSchema(img.Origin, img.Destination, o.LocalStorageFQDN)
		// if err != nil {
		// 	o.Log.Error("%v ", err)
		// }
		// // clean up the destination url
		// // for our output yaml
		// name := strings.Split(copyIS.Destination, o.LocalStorageFQDN)
		// if len(name) > 0 {
		// 	copyIS.Destination = name[1][1:]
		// }
		item := v1alpha3.DeleteItem{
			ImageName:      img.Origin,
			ImageReference: img.Destination,
		}
		if err != nil {
			o.Log.Error("%v ", err)
		}
		i, err := o.Blobs.GatherBlobs(context.Background(), img.Source)
		if err != nil {
			o.Log.Error("%v image : %s", err, i)
		}
		// physically delete blobs
		if err != nil {
			o.Log.Error(deleteImagesErrMsg, err)
		}
		var blobs []string
		for k := range i {
			// get related blobs and remove duplicates
			blobs = append(blobs, k)
			if err != nil {
				o.Log.Error("unable to write blob %s %v", k, err)
			}
		}
		sort.SliceStable(blobs, func(i, j int) bool {
			return blobs[i] < blobs[j]
		})
		item.RelatedBlobs = blobs
		items_map[img.Destination] = item
	}

	var items []v1alpha3.DeleteItem
	// convert back
	for _, v := range items_map {
		items = append(items, v)
	}
	// sort thi items
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].ImageReference < items[j].ImageReference
	})
	// marshal to yaml and write to file
	deleteImageList := v1alpha3.DeleteImageList{
		Kind:       "DeleteImageList",
		APIVersion: "mirror.openshift.io/v1alpha2",
		Items:      items,
	}
	ymlData, err := yaml.Marshal(deleteImageList)
	if err != nil {
		o.Log.Error(deleteImagesErrMsg, err)
	}
	err = os.WriteFile(filename, ymlData, 0755)
	if err != nil {
		o.Log.Error(deleteImagesErrMsg, err)
	}
	// finally copy the deleteimagesetconfig for reference
	disc := v1alpha2.DeleteImageSetConfiguration{
		TypeMeta: metav1.TypeMeta{
			Kind:       "DeleteImageSetConfiguration",
			APIVersion: "mirror.openshift.io/v1alpha2",
		},
		DeleteImageSetConfigurationSpec: v1alpha2.DeleteImageSetConfigurationSpec{
			Delete: v1alpha2.Delete{
				Platform:         o.Config.Mirror.Platform,
				Operators:        o.Config.Mirror.Operators,
				AdditionalImages: o.Config.Mirror.AdditionalImages,
			},
		},
	}
	discYamlData, err := yaml.Marshal(disc)
	if err != nil {
		o.Log.Error("%v ", err)
	}
	err = os.WriteFile(discYamlFile, discYamlData, 0755)
	if err != nil {
		o.Log.Error(deleteImagesErrMsg, err)
	}
	return nil
}

// DeleteCacheBlobs - does what it says ;)
func (o DeleteImages) DeleteCacheBlobs(images v1alpha3.DeleteImageList) error {
	o.Log.Info("deleting images from local cache")
	blobPath := filepath.Join(o.LocalStorageDisk, blobsDir, "/")
	if !o.Opts.Global.DeleteGenerate && o.Opts.Global.ForceCacheDelete {
		for _, img := range images.Items {
			for _, blob := range img.RelatedBlobs {
				digest := strings.Split(blob, "sha256:")
				if len(digest) > 1 {
					blobFile := filepath.Join(blobPath, digest[1][0:2], digest[1])
					err := os.RemoveAll(blobFile)
					if err != nil {
						o.Log.Error("unable to delete blob %s %v", blobFile, err)
					}
					o.Log.Debug("blob %s", blobFile)
				} else {
					o.Log.Warn("blob format seems to be incorrect %s", blob)
				}
			}
		}
	}
	return nil
}

// DeleteRegistryImages - does what it says ;)
func (o DeleteImages) DeleteRegistryImages(images v1alpha3.DeleteImageList) error {
	o.Log.Info("deleting images from remote registry")
	var updatedImages []v1alpha3.CopyImageSchema

	for _, img := range images.Items {
		// prefix the destination registry
		// updated := strings.Join([]string{o.Opts.Global.DeleteDestination, img.ImageReference}, "/")
		cis := v1alpha3.CopyImageSchema{
			Source:      "delete-yaml",
			Origin:      img.ImageReference,
			Destination: img.ImageReference,
		}
		o.Log.Debug("deleting images %v", cis.Destination)
		updatedImages = append(updatedImages, cis)
	}
	if !o.Opts.Global.DeleteGenerate && len(o.Opts.Global.DeleteDestination) > 0 {
		err := o.Batch.Worker(context.Background(), updatedImages, o.Opts)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadDeleteMetaData - read the list of images to delete
// used to verify the delete yaml is well formed as well as being
// the base for both local cache delete and remote registry delete
func (o DeleteImages) ReadDeleteMetaData() (v1alpha3.DeleteImageList, error) {
	var list v1alpha3.DeleteImageList
	var fileName string

	if len(o.Opts.Global.DeleteYaml) == 0 {
		fileName = filepath.Join(o.Opts.Global.WorkingDir, deleteImagesYaml)
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			return list, fmt.Errorf("delete yaml file %s does not exist (please perform a delete with --dry-run)", fileName)
		}
	} else {
		fileName = o.Opts.Global.DeleteYaml
	}

	data, err := os.ReadFile(fileName)
	if err != nil {
		return list, err
	}
	// lets parse the file to get the images
	err = yaml.Unmarshal(data, &list)
	if err != nil {
		return list, err
	}
	return list, nil
}
