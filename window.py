from typing import Optional
import numpy as np
# 此代码参考自：YYTZ.ltd

def _rolling_sum_with_count(x: np.ndarray, window_size: int):
    """Perfom a sliding window sum of the matrix.

    This is an AUXILIARY function for INTERNAL USE.

    This function will also count the the number of non-nan numbers in each
    window. Therefore, if the shape of the input matrix is [T, ...], the output
    shape of this function will be [T, ..., 2].

    output[t, ..., 0] will be the sliding window sum of the cell [t, ...], while
    output[t, ..., 1] will be the count of the non-nan numbers in the
    corresponding window.

    Note that this function will perform zero-padding at the beginning of the T
    dimension so that the output will have T rows.

    Args:

        x: A rank 2 tensor (i.e. matrix)

        window_size: the size of the sliding window

    Returns:

        A (T, ..., 2) tensor assuming the shape of the input is (T, ...). See
        above for details.

    """
    # Create a buffer of size (T + W - 1, ..., 2), assuming the shape of the
    # input matrix is (T, ...) and the window size is W. The first W - 1 rows is
    # the zero-padding.
    buf = np.zeros((*x.shape, 2))

    # Take the view of (T, ..., 2) so that we can perform the assignment.
    buf_view = buf
    # Initialize the buffer's [..., 0] by filling in x
    buf_view[..., 0] = x
    invalid = np.isnan(x)
    # The position of nan will be replaced with 0.0, so that when performing sum
    # they are not contributing.
    buf_view[invalid, 0] = 0.0
    # Initialize the buffer's [:, :, 1] by filling in 0.0 for nan and 1.0 for an
    # actual number.
    buf_view[..., 1] = (~invalid).astype(np.float32)

    # Take the sliding window view and compute the sum.
    accu = buf.cumsum(axis=0)
    accu[window_size:] = accu[window_size:] - accu[:-window_size]
    return accu


def rolling_sum(x: np.ndarray, window_size: int, min_periods: Optional[int] = None):
    """Perform a sliding window sum of the the matrix.

    Args:

        x: A rank >= 1 tensor (i.e. matrix)

        window_size: the size of the sliding window

        min_periods: if specified, any window with a count of non-nan numbers
            less than min_periods will have nan as the corresponding result.

    Returns:

        A matrix which has the same shape as the input matrix. Each cell in the
        output will hold the sum of the sliding window that ENDS at it.

    """
    buf = _rolling_sum_with_count(x, window_size)

    if min_periods is not None:
        buf[buf[..., 1] < min_periods, 0] = np.nan

    return buf[..., 0]


def rolling_mean(x: np.ndarray, window_size: int, min_periods: Optional[int] = None):
    """Perform a sliding window mean of the the matrix.

    Args:

        x: A rank >=1 tensor (i.e. matrix)

        window_size: the size of the sliding window

        min_periods: if specified, any window with a count of non-nan numbers
            less than min_periods will have nan as the corresponding result.

    Returns:

        A matrix which has the same shape as the input matrix. Each cell in the
        output will hold the mean of the sliding window that ENDS at it.

    """
    buf = _rolling_sum_with_count(x, window_size)

    if min_periods is not None:
        buf[buf[..., 1] < min_periods, 1] = np.nan

    return buf[..., 0] / buf[..., 1]


def rolling_std(x: np.ndarray, window_size: int, min_periods: Optional[int] = None):
    """Perform a sliding window standard deviation of the the matrix.

    std = sqrt(sum((xi - avg_x)^2) / n-1)
    Args:

        x: A rank >=1 tensor (i.e. matrix)

        window_size: the size of the sliding window

        min_periods: if specified, any window with a count of non-nan numbers
            less than min_periods will have nan as the corresponding result.

    Returns:
        A matrix which has the same shape as the input matrix. Each cell in the
        output will hold the std of the sliding window that ENDS at it.
    """
    assert window_size > 1, "window_size should bigger than 1"
    if min_periods is None:
        min_periods = window_size
    assert min_periods <= window_size, "window_size should bigger than min_periods"

    x_rolling_mean = rolling_mean(x, window_size, min_periods)
    # count the non-nan number in every window
    x_num = rolling_sum(~np.isnan(x), window_size=window_size, min_periods=min_periods)
    x_square_rolling_mean = rolling_mean(np.power(x, 2), window_size, min_periods)

    # Due to the precision issue, x_square_rolling_mean - np.power(x_rolling_mean, 2) might be small negative values
    # Here we lower-bound it by 0.0
    x_variance = (x_square_rolling_mean - np.power(x_rolling_mean, 2)).clip(0.0)

    # varance = sum((xi - avg_x)^2) / n
    # but we want sqrt( sum((xi - avg_x)^2) / (n - 1) ),
    # so we multiply by an adjustment factor sqrt(n/(n-1))

    index = x_num >= 2
    std = np.full_like(x_variance, np.nan, dtype=np.float64)
    std[index] = np.sqrt(x_variance[index] * x_num[index] / (x_num[index] - 1))

    return std


def rolling_cov(
    x: np.ndarray, y: np.ndarray, window_size: int, min_periods: Optional[int] = None
):
    """Perform a sliding window covarance deviation of the the matrix.

    covar = (1/(n-1))*sum((xi-avg_x)*(yi-avg_y))
    Args:


        x:  tensor (i.e. matrix  shape=TxN or Tx1)

        y:  tensor (i.e. matrix  shape=Tx1)

        window_size: the size of the sliding window

        min_periods: if specified, any window with a count of non-nan numbers
            less than min_periods will have nan as the corresponding result.

    Returns:
        A matrix which has the same shape as x matrix. Each cell in the
        output will hold the covarance of the sliding window that ENDS at it.
    """
    assert window_size > 1, "window_size should bigger than 1"
    if min_periods is None:
        min_periods = window_size
    assert min_periods <= window_size, "window_size should bigger than min_periods"
    x = np.where(~np.isnan(np.multiply(x, y)), x, np.nan)
    # process array or ndarray
    if len(y.shape) > 1 and y.shape[1] == 1:
        y = np.tile(y, (1, x.shape[1]))
    # ignore missing tuples at the same time
    y = np.where(~np.isnan(np.multiply(x, y)), y, np.nan)

    x_rolling_mean = rolling_mean(x, window_size)
    y_rolling_mean = rolling_mean(y, window_size)

    # count the non-nan number in every window
    x_num = rolling_sum(~np.isnan(np.multiply(x, y)), window_size=window_size)
    xy_rolling_mean = rolling_mean(np.multiply(x, y), window_size, min_periods)

    # covar = sum((xi - avg_x)^2) / n
    covar = xy_rolling_mean - np.multiply(x_rolling_mean, y_rolling_mean)

    # varance = (1/(n))*sum((xi-avg_x)*(yi-avg_y))
    # but we want (1/(n-1))*sum((xi-avg_x)*(yi-avg_y))
    # so we multiply by an adjustment factor sqrt(n/(n-1))

    index = x_num >= 2
    covar_adj = np.full_like(covar, np.nan, dtype=np.float64)
    covar_adj[index] = covar[index] * x_num[index] / (x_num[index] - 1)

    return covar_adj


def rolling_correlation(
    x: np.ndarray, y: np.ndarray, window_size: int, min_periods: Optional[int] = None
):
    """Perform a sliding window correlation of the the matrix.

    correlation = covar(x,y)/(std(x)*std(y))
    Args:

        x:  tensor (i.e. matrix  shape=TxN or Tx1)

        y:  tensor (i.e. matrix  shape=TxN or Tx1)

        window_size: the size of the sliding window

        min_periods: if specified, any window with a count of non-nan numbers
            less than min_periods will have nan as the corresponding result.

    Returns:
        A matrix which has the same shape as x matrix. Each cell in the
        output will hold the correlation of the sliding window that ENDS at it.
    """
    assert window_size > 1, "window_size should bigger than 1"
    if min_periods is None:
        min_periods = window_size
    assert min_periods <= window_size, "window_size should bigger than min_periods"

    finite_idxs = np.isfinite(x * y)
    x = np.where(finite_idxs, x, np.nan)
    y = np.where(finite_idxs, y, np.nan)

    x_std = rolling_std(x, window_size, min_periods)
    y_std = rolling_std(y, window_size, min_periods)

    covar = rolling_cov(x, y, window_size, min_periods)

    # correlation = covar(x,y)/(std(x)*std(y))
    index = np.multiply(x_std, y_std) != 0
    correlation = np.full_like(covar, np.nan, dtype=np.float64)
    correlation[index] = covar[index] / np.multiply(x_std[index], y_std[index])

    return correlation
