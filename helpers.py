import ee

def exportImageCollectionToGCS(imgC, bucket=None, resolution=10):
    task_ids = {}
    N = int(imgC.size().getInfo())

    for i in range(0, N):
        img = ee.Image(imgC.toList(imgC.size()).get(i))

        filename = str(img.get('FILENAME').getInfo())
        filePath = str(img.get('FILEPATH').getInfo())
        roi = ee.Geometry(img.get("ROI")).coordinates().getInfo()

        export = ee.batch.Export.image.toCloudStorage(
          image=img,
          description=filename,
          scale=resolution,
          region=roi,
          fileNamePrefix=filePath,
          bucket=bucket,
          maxPixels=1e13
        )

        # print("Exporting {} to GCS, taskID: {}".format(filename, str(export.id)))
        task_ids[season] = export.id
        export.start()

    return(task_ids)

def rescale(img, exp, thresholds):
    return img.expression(exp, {"img": img}) \
              .subtract(thresholds[0]) \
              .divide(thresholds[1] - thresholds[0])

def dilatedErossion(score, dilationPixels=3, erodePixels=1.5):
  # Perform opening on the cloud scores
  score = score \
            .reproject('EPSG:4326', None, 20) \
            .focal_min(radius=erodePixels, kernelType='circle', iterations=3) \
            .focal_max(radius=dilationPixels, kernelType='circle', iterations=3) \
            .reproject('EPSG:4326', None, 20)

  return(score)

# mergeCollection: Generates a single non-cloudy Sentinel 2 image from a processed ImageCollection
#        input: imgC - Image collection including "cloudScore" band for each image
#              threshBest - A threshold percentage to select the best image. This image is used directly as "cloudFree" if one exists.
#        output: A single cloud free mosaic for the region of interest
def mergeCollection(imgC, keepThresh=5, filterBy='CLOUDY_PERCENTAGE', filterType='less_than', mosaicBy='cloudShadowScore'):
    # Select the best images, which are below the cloud free threshold, sort them in reverse order (worst on top) for mosaicing
    best = imgC.filterMetadata(filterBy, filterType, keepThresh).sort(filterBy, False)
    filtered = imgC.qualityMosaic(mosaicBy)

    # Add the quality mosaic to fill in any missing areas of the ROI which aren't covered by good images
    newC = ee.ImageCollection.fromImages( [filtered, best.mosaic()] )

    return ee.Image(newC.mosaic())

def clipToROI(x, roi):
    return x.clip(roi).set('ROI', roi)
