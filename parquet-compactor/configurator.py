import environ
from app_common_python import LoadedConfig
from app_common_python import ObjectBuckets

ENVIRONMENT = environ.Env()


class Configurator:
    """Return the correct configurator."""

    def configurator(self):
        """Return the correct configurator based on env."""
        if ENVIRONMENT.bool("CLOWDER_ENABLED", default=False):
            return ClowderConfigurator()
        else:
            return EnvConfigurator()


class EnvConfigurator:
    """Establish S3 credentials from environment."""

    @staticmethod
    def get_object_store_endpoint():
        """Obtain object store endpoint."""
        S3_ENDPOINT = ENVIRONMENT.get_value(
            "S3_ENDPOINT", default="s3.us-east-1.amazonaws.com"
        )
        if not (
            S3_ENDPOINT.startswith("https://")
            or S3_ENDPOINT.startswith("http://")
        ):
            S3_ENDPOINT = "https://" + S3_ENDPOINT
        return S3_ENDPOINT

    @staticmethod
    def get_object_store_host():
        """Obtain object store host."""
        # return ENVIRONMENT.get_value("S3_HOST", default=None)
        pass

    @staticmethod
    def get_object_store_port():
        """Obtain object store port."""
        # return ENVIRONMENT.get_value("S3_PORT", default=443)
        pass

    @staticmethod
    def get_object_store_tls():
        """Obtain object store secret key."""
        # return ENVIRONMENT.bool("S3_SECURE", default=False)
        pass

    @staticmethod
    def get_object_store_access_key(requestedName: str = ""):
        """Obtain object store access key."""
        return ENVIRONMENT.get_value("AWS_ACCESS_KEY_ID", default=None)

    @staticmethod
    def get_object_store_secret_key(requestedName: str = ""):
        """Obtain object store secret key."""
        return ENVIRONMENT.get_value("AWS_SECRET_ACCESS_KEY", default=None)

    @staticmethod
    def get_object_store_bucket(requestedName: str = ""):
        """Obtain object store bucket."""
        return ENVIRONMENT.get_value("S3_BUCKET_NAME", default=requestedName)

    @staticmethod
    def get_data_prefix():
        return ENVIRONMENT.get_value("S3_DATA_PREFIX", default="data/parquet/")


class ClowderConfigurator:
    """Establish S3 credentials via Clowder."""

    @staticmethod
    def get_object_store_endpoint():
        """Obtain object store endpoint."""
        S3_SECURE = CLOWDER_CONFIGURATOR.get_object_store_tls()
        S3_HOST = CLOWDER_CONFIGURATOR.get_object_store_host()
        S3_PORT = CLOWDER_CONFIGURATOR.get_object_store_port()

        S3_PREFIX = "https://" if S3_SECURE else "http://"
        endpoint = f"{S3_PREFIX}{S3_HOST}"
        if bool(S3_PORT):
            endpoint += f":{S3_PORT}"
        return endpoint

    @staticmethod
    def get_object_store_host():
        """Obtain object store host."""
        return LoadedConfig.objectStore.hostname

    @staticmethod
    def get_object_store_port():
        """Obtain object store port."""
        return LoadedConfig.objectStore.port

    @staticmethod
    def get_object_store_tls():
        """Obtain object store secret key."""
        value = LoadedConfig.objectStore.tls
        if type(value) == bool:
            return value
        if value and value.lower() in ["true", "false"]:
            return value.lower() == "true"
        else:
            return False

    @staticmethod
    def get_object_store_access_key(requestedName: str = ""):
        """Obtain object store access key."""
        if requestedName != "" and ObjectBuckets.get(requestedName):
            return ObjectBuckets.get(requestedName).accessKey
        if len(LoadedConfig.objectStore.buckets) > 0:
            return LoadedConfig.objectStore.buckets[0].accessKey
        if LoadedConfig.objectStore.accessKey:
            return LoadedConfig.objectStore.accessKey

    @staticmethod
    def get_object_store_secret_key(requestedName: str = ""):
        """Obtain object store secret key."""
        if requestedName != "" and ObjectBuckets.get(requestedName):
            return ObjectBuckets.get(requestedName).secretKey
        if len(LoadedConfig.objectStore.buckets) > 0:
            return LoadedConfig.objectStore.buckets[0].secretKey
        if LoadedConfig.objectStore.secretKey:
            return LoadedConfig.objectStore.secretKey

    @staticmethod
    def get_object_store_bucket(requestedName: str = ""):
        """Obtain object store bucket."""
        if ObjectBuckets.get(requestedName):
            return ObjectBuckets.get(requestedName).name
        return requestedName

    @staticmethod
    def get_data_prefix():
        return ENVIRONMENT.get_value("S3_DATA_PREFIX", default="data/parquet/")


CLOWDER_CONFIGURATOR = ClowderConfigurator()
