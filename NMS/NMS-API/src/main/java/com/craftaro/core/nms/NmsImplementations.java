package com.craftaro.core.nms;

import com.craftaro.core.nms.anvil.AnvilCore;
import com.craftaro.core.nms.entity.NMSPlayer;
import com.craftaro.core.nms.world.NmsWorldBorder;
import com.craftaro.core.nms.world.WorldCore;
import com.craftaro.core.nms.nbt.NBTCore;
import org.jetbrains.annotations.NotNull;

public interface NmsImplementations {
    @NotNull NMSPlayer getPlayer();

    @NotNull WorldCore getWorld();

    @NotNull NmsWorldBorder getWorldBorder();

    @NotNull AnvilCore getAnvil();

    @NotNull NBTCore getNbt();
}