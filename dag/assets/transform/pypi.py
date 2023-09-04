# imports
import ibis
import dagster

from dag import functions as f


# assets
@dagster.asset
def transform_downloads(extract_downloads):
    """
    Transform the PyPI downloads data.
    """
    downloads = f.clean_data(
        extract_downloads.drop("project").unpack("file").unpack("details")
    )
    downloads = downloads.mutate(
        major_minor_patch=f.clean_version(downloads["version"], patch=True),
        major_minor=f.clean_version(downloads["version"], patch=False).cast("float"),
    )
    downloads = downloads.rename(
        {
            "version_raw": "version",
            "version": "major_minor_patch",
        }
    )
    downloads = (
        downloads.group_by(
            [
                ibis._.timestamp.truncate("D").name("timestamp"),
                ibis._.country_code,
                ibis._.version,
                ibis._.python,
                ibis._.system["name"].name("system"),
            ]
        )
        .agg(
            ibis._.count().name("downloads"),
        )
        .order_by(ibis._.timestamp.desc())
        .mutate(
            ibis._.downloads.sum()
            .over(
                rows=(0, None),
                group_by=["country_code", "version", "python", "system"],
                order_by=ibis._.timestamp.desc(),
            )
            .name("total_downloads")
        )
        .order_by(ibis._.timestamp.desc())
    )
    downloads = downloads.mutate(ibis._["python"].fillna("").name("python_full"))
    downloads = downloads.mutate(
        f.clean_version(downloads["python_full"], patch=False).name("python")
    )
    return downloads
