defmodule Kaffe.PartitionSelector do
  @doc """
  Cycle current from 0 to total-1.

  ## Examples

  iex> Kaffe.PartitionSelector.round_robin(nil, 3)
  0

  iex> Kaffe.PartitionSelector.round_robin(0, 3)
  1

  iex> Kaffe.PartitionSelector.round_robin(1, 3)
  2

  iex> Kaffe.PartitionSelector.round_robin(2, 3)
  0
  """
  def round_robin(current, total) do
    if current < total - 1 do
      current + 1
    else
      0
    end
  end

  def random(total) do
    :crypto.rand_uniform(0, total)
  end

  def md5(key, total) do
    :crypto.hash(:md5, key)
    |> :binary.bin_to_list
    |> Enum.sum
    |> rem(total)
  end
end
