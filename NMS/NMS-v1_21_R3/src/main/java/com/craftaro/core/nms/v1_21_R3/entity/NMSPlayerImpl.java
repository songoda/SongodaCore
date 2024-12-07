package com.craftaro.core.nms.v1_21_R3.entity;

import com.craftaro.core.nms.entity.NMSPlayer;
import com.craftaro.core.nms.entity.player.GameProfile;
import com.mojang.authlib.properties.Property;
import net.minecraft.network.protocol.Packet;
import net.minecraft.world.item.component.ResolvableProfile;
import org.bukkit.craftbukkit.v1_21_R3.entity.CraftPlayer;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

public class NMSPlayerImpl implements NMSPlayer {
    @Override
    public void sendPacket(Player p, Object packet) {
        ((CraftPlayer) p).getHandle().connection.send((Packet<?>) packet);
    }

    @Override
    public GameProfile getProfile(Player p) {
        com.mojang.authlib.GameProfile profile = ((CraftPlayer) p).getHandle().getGameProfile();
        return wrapProfile(profile);
    }

    @Override
    public GameProfile createProfile(UUID id, String name, @Nullable String textureValue, @Nullable String textureSignature) {
        com.mojang.authlib.GameProfile profile = new com.mojang.authlib.GameProfile(id, name);
        if (textureValue != null) {
            profile.getProperties().put("textures", new Property("textures", textureValue, textureSignature));
        }

        return wrapProfile(profile);
    }

    private GameProfile wrapProfile(com.mojang.authlib.GameProfile profile) {
        String textureValue = null;
        String textureSignature = null;
        for (Property property : profile.getProperties().get("textures")) {
            if (property.name().equals("textures")) {
                textureValue = property.value();
                textureSignature = property.signature();
            }
        }

        return new GameProfile(
                profile,
                new ResolvableProfile(profile),

                profile.getId(),
                profile.getName(),
                textureValue,
                textureSignature
        );
    }
}
