class InsufficientTokensError(Exception):
    def __init__(self):
        self.message = "Insufficient tokens to purchase."
        super().__init__(self.message)


class ForcedFailureError(Exception):
    def __init__(self):
        self.message = "Failure in inventory service!"
        super().__init__(self.message)
