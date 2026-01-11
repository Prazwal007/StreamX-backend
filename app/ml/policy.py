import numpy as np
import random

class ContextualBandit:
    def __init__(self, n_actions: int, n_features: int, epsilon=0.1):
        self.n_actions = n_actions
        self.n_features = n_features
        self.epsilon = epsilon

        self.weights = np.zeros((n_actions, n_features))
        self.counts = np.zeros(n_actions)

    def select_action(self, context: np.ndarray) -> int:
        if random.random() < self.epsilon:
            return random.randint(0, self.n_actions - 1)

        scores = self.weights @ context
        return int(np.argmax(scores))

    def update(self, action: int, context: np.ndarray, reward: float):
        self.counts[action] += 1
        lr = 1.0 / self.counts[action]
        self.weights[action] += lr * reward * context
        self.epsilon = max(0.01, self.epsilon * 0.999) #for fater learning
