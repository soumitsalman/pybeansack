if __name__ in {"__main__", "__mp_main__"}:
    from app.web import router
    router.run()
