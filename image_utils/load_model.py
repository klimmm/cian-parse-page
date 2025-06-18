import tensorflow as tf
from tensorflow.keras import layers, Model
from tensorflow.keras.applications import EfficientNetB0

# Constants
IMG_SIZE = (224, 224)


def focal_loss(gamma=2.0, alpha=0.25):
    """Define focal loss for model loading"""

    def loss(y_true, y_pred):
        pt_1 = tf.where(tf.equal(y_true, 1), y_pred, tf.ones_like(y_pred))
        pt_0 = tf.where(tf.equal(y_true, 0), y_pred, tf.zeros_like(y_pred))
        eps = tf.keras.backend.epsilon()
        pt_1 = tf.clip_by_value(pt_1, eps, 1 - eps)
        pt_0 = tf.clip_by_value(pt_0, eps, 1 - eps)
        return -tf.reduce_sum(
            alpha * tf.pow(1 - pt_1, gamma) * tf.math.log(pt_1)
        ) - tf.reduce_sum((1 - alpha) * tf.pow(pt_0, gamma) * tf.math.log(1 - pt_0))

    return loss


def build_model():
    """Build the model architecture from training script"""
    # Input layer
    inputs = layers.Input(shape=IMG_SIZE + (3,))

    # Preprocessing Lambda layer with explicit output shape
    x = layers.Lambda(
        lambda img: tf.keras.applications.efficientnet.preprocess_input(img),
        output_shape=lambda x: x,
    )(inputs)

    # Base model
    base = EfficientNetB0(include_top=False, weights=None, input_shape=IMG_SIZE + (3,))
    base.trainable = True  # Set to match your fine-tuning phase

    # Freeze early layers (first 150 layers)
    for layer in base.layers[:150]:
        layer.trainable = False

    # Freeze all BatchNormalization layers
    for layer in base.layers:
        if isinstance(layer, layers.BatchNormalization):
            layer.trainable = False

    # Connect base model
    x = base(x, training=False)

    # Classification head
    x = layers.GlobalAveragePooling2D()(x)
    x = layers.BatchNormalization()(x)
    x = layers.Dense(
        128,
        kernel_initializer="he_normal",
        kernel_regularizer=tf.keras.regularizers.l2(1e-4),
    )(x)
    x = layers.BatchNormalization()(x)
    x = layers.Activation("relu")(x)
    x = layers.Dropout(0.5)(x)
    outputs = layers.Dense(
        1,
        activation="sigmoid",
        dtype="float32",
        kernel_initializer="glorot_normal",
        bias_initializer=tf.keras.initializers.Constant(-0.5),
    )(x)

    model = Model(inputs, outputs)
    model.compile(
        optimizer=tf.keras.optimizers.Adam(1e-5),
        loss=focal_loss(),
        metrics=["accuracy"],
    )

    return model


def load_model(model_path):
    """Load the trained model by rebuilding architecture and loading weights"""
    try:
        print("Loading model by rebuilding architecture and loading weights...")
        model = build_model()
        model.load_weights(model_path)
        print("Success! Loaded weights into rebuilt model.")
        return model
    except Exception as e:
        print(f"Failed to load model: {e}")
        return None