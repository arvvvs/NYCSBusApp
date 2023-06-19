from data.metric_generation_upload import METRIC_GENERATION_UPLOAD


def main():
    [metric_generation_func() for metric_generation_func in METRIC_GENERATION_UPLOAD]


if __name__ == "__main__":
    main()
