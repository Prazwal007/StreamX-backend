from .policy import ContextualBandit
from .feature import extract_features
from .storage import save_model, load_model

class MLController:
    def __init__(self):
        self.bandit = load_model() or ContextualBandit(
            n_actions=5,
            n_features=6
        )
        self.last_context = None
        self.last_action = None

    def decide(self, metrics: dict):
        context = extract_features(metrics)
        action = self.bandit.select_action(context)
        self.last_context = context
        self.last_action = action
        return action

    def feedback(self, reward: float):
        if self.last_context is not None:
            self.bandit.update(
                self.last_action,
                self.last_context,
                reward
            )
            save_model(self.bandit)
