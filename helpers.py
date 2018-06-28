import ee

def exportImageCollectionToGCS(imgC, bucket=None, resolution=10, start=False):
    task_ids = {}
    N = int(imgC.size().getInfo())

    for i in range(0, N):
        img = ee.Image(imgC.toList(imgC.size()).get(i))

        filename = str(img.get('FILENAME').getInfo())
        filePath = str(img.get('FILEPATH').getInfo())
        roi = ee.Geometry(img.get("ROI"))
        roi = roi.coordinates().getInfo()

        export = exportImageToGCS(img=img, roi=roi, bucket=bucket, resolution=resolution, filename=filename, dest_path=filePath, start=start)
        task_ids[filename] = export.id

    return(task_ids)

def exportImageToGCS(img=None, roi=None, bucket=None, filename=None, dest_path=None, resolution=10, start=False):

    export = ee.batch.Export.image.toCloudStorage(
      image=img,
      description=filename,
      scale=resolution,
      region=roi,
      fileNamePrefix=dest_path,
      bucket=bucket,
      maxPixels=1e13
    )

    if start:
        export.start()

    return(export)

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

# calcCloudCoverage: Calculates a mask for clouds in the image.
#        input: im - Image from image collection with a valid mask layer
#        output: original image with added stats.
#                - CLOUDY_PERCENTAGE: The percentage of the image area affected by clouds
#                - ROI_COVERAGE_PERCENT: The percentage of the ROI region this particular image covers
#                - CLOUDY_PERCENTAGE_ROI: The percentage of the original ROI which is affected by the clouds in this image
#                - cloudScore: A per pixel score of cloudiness
def calcCloudCoverage(img, cloudThresh=0.2):
    imgPoly = ee.Algorithms.GeometryConstructors.Polygon(
              ee.Geometry( img.get('system:footprint') ).coordinates()
              )

    roi = ee.Geometry(img.get('ROI'))

    intersection = roi.intersection(imgPoly, ee.ErrorMargin(0.5))
    cloudMask = img.select(['cloudScore']).gt(cloudThresh).clip(roi).rename('cloudMask')

    cloudAreaImg = cloudMask.multiply(ee.Image.pixelArea())

    stats = cloudAreaImg.reduceRegion(
      reducer=ee.Reducer.sum(),
      geometry=roi,
      scale=10,
      maxPixels=1e12,
      bestEffort=True,
      tileScale=16
    )

    maxAreaError = 10
    cloudPercent = ee.Number(stats.get('cloudMask')).divide(imgPoly.area(maxAreaError)).multiply(100)
    coveragePercent = ee.Number(intersection.area(maxAreaError)).divide(roi.area(maxAreaError)).multiply(100)
    cloudPercentROI = ee.Number(stats.get('cloudMask')).divide(roi.area(maxAreaError)).multiply(100)

    img = img.set('CLOUDY_PERCENTAGE', cloudPercent)
    img = img.set('ROI_COVERAGE_PERCENT', coveragePercent)
    img = img.set('CLOUDY_PERCENTAGE_ROI', cloudPercentROI)

    return img

def clipToROI(x, roi):
    return x.clip(roi).set('ROI', roi)
