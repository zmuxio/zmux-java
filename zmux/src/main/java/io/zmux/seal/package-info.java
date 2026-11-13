/**
 * The secure carrier's Establish: {@link io.zmux.seal.Seal#establish} on the client with a
 * {@link io.zmux.seal.ClientConfig}, {@link io.zmux.seal.Seal#server} as a listener's inbound transform with a
 * {@link io.zmux.seal.ServerConfig}, and the host's trust interfaces: {@link io.zmux.seal.Anchor} (with its
 * {@link io.zmux.seal.VerifyInput} and {@link io.zmux.seal.VerifyVerdict}), {@link io.zmux.seal.Gate} and
 * {@link io.zmux.seal.Signer}. Keys are {@link io.zmux.seal.StaticKey} and {@link io.zmux.seal.SigningKey}, named by
 * {@link io.zmux.seal.KeyId}; what a handshake learned of the other end is a {@link io.zmux.seal.Peer}. Failures of
 * the protocol are {@link io.zmux.seal.SecureCarrierException}. Byte arrays in these types are copied in and out.
 * Join is the core's and needs none of this.
 */
package io.zmux.seal;
