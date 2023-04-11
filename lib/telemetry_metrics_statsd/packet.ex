defmodule TelemetryMetricsStatsd.Packet do
  @moduledoc false

  require Logger
  alias __MODULE__

  @type t :: %__MODULE__{
          max_bytes: integer(),
          max_age_micro: integer(),
          send_fun: function(),
          data: iodata(),
          bytes: integer(),
          birth_micro: integer()
        }

  defstruct max_bytes: nil,
            max_age_micro: nil,
            send_fun: nil,
            data: [],
            bytes: 0,
            birth_micro: nil

  @spec new(max_bytes :: integer(), max_age_micro :: integer(), send_fun :: function()) :: Packet.t()
  def new(max_bytes, max_age_micro, send_fun) do
    do_new(max_bytes, max_age_micro, send_fun)
  end

  @spec get_timeout(Packet.t()) :: integer()
  def get_timeout(%Packet{max_age_micro: max_age_micro, birth_micro: birth_micro}) do
    case max_age_micro - (now() - birth_micro) do
      t when t < 1000 -> 0
      t -> div(t, 1000)
    end
  end

  @spec maybe_send(Packet.t(), iodata()) :: Packet.t()
  def maybe_send(packet, line) when is_list(line) do
    maybe_send(packet, IO.iodata_to_binary(line))
  end

  def maybe_send(packet, "") do
    maybe_send(packet)
  end

  def maybe_send(%Packet{max_bytes: max_bytes} = packet, line) when byte_size(line) > max_bytes do
    Logger.error("discarded due to packet size overflow:\nmetric=#{inspect(line)}")
    maybe_send(packet)
  end

  def maybe_send(%Packet{bytes: bytes, data: data, max_bytes: max_bytes} = packet, line) when is_binary(line) do
    new_bytes = concatenated_size(bytes, byte_size(line))
    case new_bytes > max_bytes do
      true ->
        new_packet = flush_send(packet)
        maybe_send(new_packet, line)
      false ->
        maybe_send(%Packet{packet | data: concatenate(data, line), bytes: new_bytes}, line)
    end
  end

  @spec flush_send(Packet.t()) :: Packet.t()
  def flush_send(%Packet{data: data, bytes: bytes, max_bytes: max_bytes, max_age_micro: max_age_micro, send_fun: send_fun}) do
    bytes > 0 && send_fun.(data)
    do_new(max_bytes, max_age_micro, send_fun)
  end

  @spec build_packets([binary()], size :: non_neg_integer(), joiner :: binary()) :: [binary()]
  def build_packets(binaries, max_size, joiner)
      when is_integer(max_size) and max_size > 0 and is_binary(joiner) do
    build_packets(binaries, max_size, {joiner, byte_size(joiner)}, [{[], 0, 0}])
  end

  # Only the first element of `acc` is a pair of packet and its size.
  def build_packets([], _, {joiner, _}, [{packet_binaries, _, _} | acc]) do
    packet =
      packet_binaries
      |> :lists.reverse()
      |> Enum.intersperse(joiner)
      |> :erlang.iolist_to_binary()

    :lists.reverse([packet | acc])
  end

  def build_packets([binary | binaries], max_size, {joiner, joiner_size}, [
        {packet_binaries, packet_binaries_count, packet_binaries_size} | acc
      ]) do
    binary_size = byte_size(binary)

    if binary_size > max_size do
      # TODO: this should be probably handled in a nicer way
      raise "Binary size exceeds the provided maximum packet size. You might increase it via the :mtu config."
    end

    new_packet_binaries_count = packet_binaries_count + 1
    new_packet_binaries_size = packet_binaries_size + binary_size
    packet_size = new_packet_binaries_size + (new_packet_binaries_count - 1) * joiner_size

    if packet_size <= max_size do
      packet_binaries = [binary | packet_binaries]

      build_packets(binaries, max_size, {joiner, joiner_size}, [
        {packet_binaries, new_packet_binaries_count, new_packet_binaries_size} | acc
      ])
    else
      packet =
        packet_binaries
        |> :lists.reverse()
        |> Enum.intersperse(joiner)
        |> :erlang.iolist_to_binary()

      build_packets([binary | binaries], max_size, {joiner, joiner_size}, [
        {[], 0, 0},
        packet | acc
      ])
    end
  end

  defp maybe_send(packet) do
    case get_timeout(packet) == 0 do
      true  -> flush_send(packet)
      false -> packet
    end
  end


  defp concatenated_size(0, new_line_bytes), do: new_line_bytes
  defp concatenated_size(bytes, new_line_bytes), do: bytes + new_line_bytes + 1

  defp concatenate([], line), do: [line]
  defp concatenate(acc_lines, line), do: [acc_lines, ?\n, line]

  defp now(), do: System.system_time(:microsecond)

  defp do_new(max_bytes, max_age_micro, send_fun) do
    %Packet{max_bytes: max_bytes, max_age_micro: max_age_micro, send_fun: send_fun, birth_micro: now()}
  end
end
