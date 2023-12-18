// Package azurestore provides a Azure Blob Storage based backend

// AzureStore is a storage backend that uses the AzService interface in order to store uploads in Azure Blob Storage.
// It stores the uploads in a container specified in two different BlockBlob: The `[id].info` blobs are used to store the fileinfo in JSON format. The `[id]` blobs without an extension contain the raw binary data uploaded.
// If the upload is not finished within a week, the uncommited blocks will be discarded.

// Support for setting the default Continaer access type and Blob access tier varies on your Azure Storage Account and its limits.
// More information about Container access types and limts
// https://docs.microsoft.com/en-us/azure/storage/blobs/anonymous-read-access-configure?tabs=portal

// More information about Blob access tiers and limits
// https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-performance-tiers
// https://docs.microsoft.com/en-us/azure/storage/common/storage-account-overview#access-tiers-for-block-blob-data

package azurestore

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"

	"github.com/tus/tusd/v2/pkg/handler"
)

const (
	InfoBlobSuffix   string = ".info"
	MaxBlockBlobSize int64  = blockblob.MaxBlocks * blockblob.MaxStageBlockBytes
	// MaxBlockBlobChunkSize int64  = blockblob.MaxBlockBlobChunkSize
)

type azService struct {
	BlobAccessTier blob.AccessTier
	// ContainerURL   *azblob.ContainerURL
	Client        *service.Client
	Credentials   *service.SharedKeyCredential
	ContainerName string
}

type AzService interface {
	NewBlob(ctx context.Context, name string) (AzBlob, error)
}

type AzConfig struct {
	AccountName         string
	AccountKey          string
	BlobAccessTier      string
	ContainerName       string
	ContainerAccessType string
	Endpoint            string
}

type AzBlob interface {
	// Delete the blob
	Delete(ctx context.Context) error
	// Upload the blob
	Upload(ctx context.Context, body io.ReadSeeker) error
	// Download returns a readcloser to download the contents of the blob
	Download(ctx context.Context) (io.ReadCloser, error)
	// Get the offset of the blob and its indexes
	GetOffset(ctx context.Context) (int64, error)
	// Commit the uploaded blocks to the BlockBlob
	Commit(ctx context.Context) error
	// Append Url
	StageBlockFromURL(ctx context.Context, srcUrl string) error
	// Get blob sas url url
	GetSASURL(expiry time.Duration) string
}

type BlockBlob struct {
	Blob       *blockblob.Client
	AccessTier blob.AccessTier
	Indexes    []int
}

type InfoBlob struct {
	Blob       *blockblob.Client
	AccessTier blob.AccessTier
}

// New Azure service for communication to Azure BlockBlob Storage API
func NewAzureService(config *AzConfig) (AzService, error) {

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", config.AccountName)

	cred, err := service.NewSharedKeyCredential(config.AccountName, config.AccountKey)
	if err != nil {
		return nil, err
	}
	serviceClient, err := service.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return nil, err
	}

	accessTiers := []blob.AccessTier{blob.AccessTierArchive, blob.AccessTierCool, blob.AccessTierHot}

	//default to "hot"
	blobAccessTierType := accessTiers[2]
	switch config.BlobAccessTier {
	case "archive":
		blobAccessTierType = accessTiers[0]
	case "cool":
		blobAccessTierType = accessTiers[1]
	case "hot":
		blobAccessTierType = accessTiers[2]
	case "":
	default:
		blobAccessTierType = accessTiers[2]
	}

	return &azService{
		BlobAccessTier: blobAccessTierType,
		Client:         serviceClient,
		Credentials:    cred,
		ContainerName:  config.ContainerName,
	}, nil
}

// Determine if we return a InfoBlob or BlockBlob, based on the name
func (service *azService) NewBlob(ctx context.Context, name string) (AzBlob, error) {
	blockBlobClient, err := blockblob.NewClientWithSharedKeyCredential(fmt.Sprintf("%s%s/%s", service.Client.URL(), service.ContainerName, name), service.Credentials, &blockblob.ClientOptions{})
	if err != nil {
		return nil, err
	}

	var fileBlob AzBlob
	if strings.HasSuffix(name, InfoBlobSuffix) {
		fileBlob = &InfoBlob{
			Blob:       blockBlobClient,
			AccessTier: service.BlobAccessTier,
		}
	} else {
		fileBlob = &BlockBlob{
			Blob:       blockBlobClient,
			Indexes:    []int{},
			AccessTier: service.BlobAccessTier,
		}
	}

	return fileBlob, nil
}

// Delete the blockBlob from Azure Blob Storage
func (bb *BlockBlob) Delete(ctx context.Context) error {
	_, err := bb.Blob.Delete(ctx, nil)
	return err
}

// Upload a block to Azure Blob Storage and add it to the indexes to be after upload is finished
func (bb *BlockBlob) Upload(ctx context.Context, body io.ReadSeeker) error {
	// Keep track of the indexes
	var index int
	if len(bb.Indexes) == 0 {
		index = 0
	} else {
		index = bb.Indexes[len(bb.Indexes)-1] + 1
	}
	bb.Indexes = append(bb.Indexes, index)

	uploadOptions := &blockblob.StageBlockOptions{
		LeaseAccessConditions: &blob.LeaseAccessConditions{},
	}
	rsc := streaming.NopCloser(body)
	_, _ = rsc.Seek(0, io.SeekStart)

	_, err := bb.Blob.StageBlock(ctx, blockIDIntToBase64(index), rsc, uploadOptions)
	if err != nil {
		return err
	}
	return nil
}

// Get the SAS url for a BlockBlob
func (bb *BlockBlob) GetSASURL(expiry time.Duration) string {

	test, _ := bb.Blob.GetSASURL(sas.BlobPermissions{Read: true, List: true}, time.Now().UTC().Add(expiry), nil)

	return test
}

// Stage Block from url. used to combine the parts into the final one using sasurls
func (bb *BlockBlob) StageBlockFromURL(ctx context.Context, srcUrl string) error {
	// Keep track of the indexes
	var index int
	if len(bb.Indexes) == 0 {
		index = 0
	} else {
		index = bb.Indexes[len(bb.Indexes)-1] + 1
	}
	bb.Indexes = append(bb.Indexes, index)

	_, err := bb.Blob.StageBlockFromURL(ctx, blockIDIntToBase64(index), srcUrl, &blockblob.StageBlockFromURLOptions{})
	if err != nil {
		return err
	}
	return nil
}

// Download the blockBlob from Azure Blob Storage
func (bb *BlockBlob) Download(ctx context.Context) (io.ReadCloser, error) {

	downloadResponse, err := bb.Blob.DownloadStream(ctx, &blob.DownloadStreamOptions{})

	if err != nil {
		// This might occur when the blob is being uploaded, but a block list has not been committed yet
		if isAzureError(err, "BlobNotFound") {
			err = handler.ErrNotFound
		}
		return nil, err
	}

	return downloadResponse.Body, nil
}

func (bb *BlockBlob) GetOffset(ctx context.Context) (int64, error) {
	// Get the offset of the file from azure storage
	// For the blob, show each block (ID and size) that is a committed part of it.
	var indexes []int
	var offset int64

	resp, err := bb.Blob.GetBlockList(ctx, blockblob.BlockListTypeAll, &blockblob.GetBlockListOptions{
		AccessConditions: &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{}},
	})
	if err != nil {
		if isAzureError(err, "BlobNotFound") {
			err = handler.ErrNotFound
		}

		//Todo: if the response is BlobNotFound, return index 0 and no error.  The blob should get created. handle other errors normally
		return 0, nil
	}

	// Need committed blocks to be added to offset to know how big the file really is
	for _, block := range resp.BlockList.CommittedBlocks {
		offset += int64(*block.Size)
		indexes = append(indexes, blockIDBase64ToInt(*block.Name))
	}

	// Need to get the uncommitted blocks so that we can commit them
	for _, block := range resp.BlockList.UncommittedBlocks {
		offset += int64(*block.Size)
		indexes = append(indexes, blockIDBase64ToInt(*block.Name))
	}

	// Sort the block IDs in ascending order. This is required as Azure returns the block lists alphabetically
	// and we store the indexes as base64 encoded ints.
	sort.Ints(indexes)
	bb.Indexes = indexes

	return offset, nil
}

// After all the blocks have been uploaded, we commit the unstaged blocks by sending a Block List
func (bb *BlockBlob) Commit(ctx context.Context) error {
	base64BlockIDs := make([]string, len(bb.Indexes))
	for index, id := range bb.Indexes {
		base64BlockIDs[index] = blockIDIntToBase64(id)
	}

	_, err := bb.Blob.CommitBlockList(ctx, base64BlockIDs, &blockblob.CommitBlockListOptions{})
	return err
}

// Delete the infoBlob from Azure Blob Storage
func (ib *InfoBlob) Delete(ctx context.Context) error {
	_, err := ib.Blob.Delete(ctx, nil)
	return err
}

// Upload the infoBlob to Azure Blob Storage
// Because the info file is presumed to be smaller than azblob.BlockBlobMaxUploadBlobBytes (256MiB), we can upload it all in one go
// New uploaded data will create a new, or overwrite the existing block blob
func (ib *InfoBlob) Upload(ctx context.Context, body io.ReadSeeker) error {
	uploadOptions := blockblob.UploadOptions{
		HTTPHeaders:             &blob.HTTPHeaders{},
		TransactionalContentMD5: nil,
		Tier:                    &ib.AccessTier,
	}

	_, err := ib.Blob.Upload(ctx, streaming.NopCloser(body), &uploadOptions)
	return err
}

// infoBlob does not utilise GetSASURL, so just return ""
func (ib *InfoBlob) GetSASURL(expiry time.Duration) string {
	//we don't care about a sas url here, so just return empty string
	return ""
}

// infoBlob does not utilise StageBlockFromURL, so just return nil
func (ib *InfoBlob) StageBlockFromURL(ctx context.Context, srcUrl string) error {
	return nil
}

// Download the infoBlob from Azure Blob Storage
func (ib *InfoBlob) Download(ctx context.Context) (io.ReadCloser, error) {
	downloadResponse, err := ib.Blob.DownloadStream(ctx, &blob.DownloadStreamOptions{})

	if err != nil {
		if isAzureError(err, "BlobNotFound") {
			err = handler.ErrNotFound
		}
		return nil, err
	}

	return downloadResponse.Body, nil
}

// infoBlob does not utilise offset, so just return 0, nil
func (infoBlob *InfoBlob) GetOffset(ctx context.Context) (int64, error) {
	return 0, nil
}

// infoBlob does not have uncommited blocks, so just return nil
func (infoBlob *InfoBlob) Commit(ctx context.Context) error {
	return nil
}

// === Helper Functions ===
// These helper functions convert a binary block ID to a base-64 string and vice versa
// NOTE: The blockID must be <= 64 bytes and ALL blockIDs for the block must be the same length
func blockIDBinaryToBase64(blockID []byte) string {
	return base64.StdEncoding.EncodeToString(blockID)
}

func blockIDBase64ToBinary(blockID string) []byte {
	binary, _ := base64.StdEncoding.DecodeString(blockID)
	return binary
}

// These helper functions convert an int block ID to a base-64 string and vice versa
func blockIDIntToBase64(blockID int) string {
	binaryBlockID := (&[4]byte{})[:] // All block IDs are 4 bytes long
	binary.LittleEndian.PutUint32(binaryBlockID, uint32(blockID))
	return blockIDBinaryToBase64(binaryBlockID)
}

func blockIDBase64ToInt(blockID string) int {
	blockIDBase64ToBinary(blockID)
	return int(binary.LittleEndian.Uint32(blockIDBase64ToBinary(blockID)))
}

func isAzureError(err error, code string) bool {
	//Todo: handle azure errors
	// if err, ok := err.(*azblob); ok && string(err.ServiceCode()) == code {
	// 	return true
	// }
	// return false
	return false
}
