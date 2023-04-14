defmodule TelemetryMetricsStatsd.UDP do
  @moduledoc false

  use GenServer
  require Logger
  alias TelemetryMetricsStatsd.Packet

  defstruct [:host, :port, :socket, :packet]

  @opaque t :: %__MODULE__{
            host: :inet.hostname() | :inet.ip_address() | :inet.local_address(),
            port: :inet.port_number(),
            socket: :gen_udp.socket(),
            packet: Packet.t()
          }

  @type config :: %{
          host: :inet.hostname() | :inet.ip_address() | :inet.local_address(),
          port: :inet.port_number(),
          mtu: non_neg_integer(),
          max_report_interval_ms: non_neg_integer()
        }

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  @spec send(GenServer.name(), iodata) :: :ok
  def send(pid, data) do
    GenServer.cast(pid, {:send, data})
  end

  def update(pid, new_host, new_port) do
    GenServer.cast(pid, {:update, new_host, new_port})
  end

  def close(pid) do
    GenServer.call(pid, :close)
  end

  def stop(pid, reason) do
    GenServer.stop(pid, reason)
  end

  @impl true
  @spec init(config :: config) :: {:ok, __MODULE__.t(), timeout()}
  def init(config) do
    host = config.host

    case open(host) do
      {:ok, socket} ->
        send_fun = make_send_fun(self(), socket, host, config.port)

        packet = Packet.new(config.mtu, config.max_report_interval_ms * 1000, send_fun)
        state = struct(__MODULE__, Map.merge(config, %{socket: socket, packet: packet}))
        {:ok, state, Packet.get_timeout(packet)}
    end
  end

  @impl true
  def handle_call(:close, _from, %__MODULE__{socket: socket, packet: packet} = state) do
    Packet.flush_send(packet)
    :gen_udp.close(socket)
    {:stop, :close, :ok, state}
  end

  @impl true
  def handle_cast({:update, new_host, new_port}, %__MODULE__{socket: socket, packet: packet} = state) do
    new_packet = %Packet{packet | send_fun: make_send_fun(self(), socket, new_host, new_port)}
    noreply(%__MODULE__{state | host: new_host, port: new_port, packet: new_packet})
  end

  @impl true
  def handle_cast({:send, data}, %{packet: packet} = state) do
    noreply(Map.put(state, :packet, Packet.maybe_send(packet, data)))
  end

  @impl true
  def handle_info(:timeout, %{packet: packet} = state) do
    noreply(Map.put(state, :packet, Packet.flush_send(packet)))
  end

  defp noreply(%{packet: packet} = state) do
    {:noreply, state, Packet.get_timeout(packet)}
  end

  defp open(host) do
    default_opts = [active: false]

    opts =
      case host do
        {:local, _} ->
          [:local | default_opts]

        _ ->
          default_opts
      end

    case :gen_udp.open(0, opts) do
      {:ok, socket} ->
        {:ok, socket}

      {:error, _} = err ->
        err
    end
  end

  defp make_send_fun(socket_owner, socket, {:local, _} = host, _port) do
    fn data ->
      do_send(socket_owner, socket, host, 0, data)
    end
  end

  defp make_send_fun(socket_owner, socket, host, port) do
    fn data ->
      do_send(socket_owner, socket, host, port, data)
    end
  end

  defp do_send(socket_owner, socket, host, port, data) do
    case :gen_udp.send(socket, host, port, data) do
      :ok ->
        :ok

      {:error, reason} = error ->
        TelemetryMetricsStatsd.udp_error(socket_owner, reason)
        error
    end
  end
end
